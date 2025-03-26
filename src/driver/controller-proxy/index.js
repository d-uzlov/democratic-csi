const _ = require("lodash");
const semver = require("semver");
const { CsiBaseDriver } = require("../index");
const yaml = require("js-yaml");
const fs = require('fs');
const { Registry } = require("../../utils/registry");
const { GrpcError, grpc } = require("../../utils/grpc");
const path = require('path');
const os = require("os");

const volumeIdPrefix = 'v:';
const snapshotIdPrefix = 's:';
const TOPOLOGY_DEFAULT_PREFIX = 'org.democratic-csi.topology';

class CsiProxyDriver extends CsiBaseDriver {
  constructor(ctx, options) {
    super(...arguments);
    this.options.proxy.configFolder = path.normalize(this.options.proxy.configFolder);
    if (this.options.proxy.configFolder.slice(-1) == '/') {
      this.options.proxy.configFolder = this.options.proxy.configFolder.slice(0, -1);
    }
    this.nodeIdSerializer = new NodeIdSerializer(ctx, options.proxy.nodeId || {});

    // corresponding storage class could be deleted without notice
    // let's delete entry from cache after 1 hour, so it can be cleaned by GC
    // one hour seems long enough to avoid recreating frequently used drivers
    // creating a new instance after long inactive period shouldn't be a problem
    const oneMinuteInMs = 1000 * 60;
    this.enableCacheTimeout = this.options.proxy.cacheTimeoutMinutes != -1;
    this.cacheTimeout = (this.options.proxy.cacheTimeoutMinutes ?? 60) * oneMinuteInMs;
    if (!this.enableCacheTimeout) {
      this.ctx.logger.info("driver cache is permanent");
    } else {
      this.ctx.logger.info(`driver cache timeout is ${this.options.proxy.cacheTimeoutMinutes} minutes`);
    }

    options = options || {};
    options.service = options.service || {};
    options.service.identity = options.service.identity || {};
    options.service.controller = options.service.controller || {};
    options.service.node = options.service.node || {};

    options.service.identity.capabilities =
      options.service.identity.capabilities || {};

    options.service.controller.capabilities =
      options.service.controller.capabilities || {};

    options.service.node.capabilities = options.service.node.capabilities || {};

    if (!("service" in options.service.identity.capabilities)) {
      this.ctx.logger.debug("setting default identity service caps");

      options.service.identity.capabilities.service = [
        //"UNKNOWN",
        "CONTROLLER_SERVICE",
        "VOLUME_ACCESSIBILITY_CONSTRAINTS",
      ];
    }

    if (!("volume_expansion" in options.service.identity.capabilities)) {
      this.ctx.logger.debug("setting default identity volume_expansion caps");

      options.service.identity.capabilities.volume_expansion = [
        //"UNKNOWN",
        "ONLINE",
        //"OFFLINE"
      ];
    }

    if (!("rpc" in options.service.controller.capabilities)) {
      this.ctx.logger.debug("setting default controller caps");

      options.service.controller.capabilities.rpc = [
        //"UNKNOWN",
        "CREATE_DELETE_VOLUME",
        //"PUBLISH_UNPUBLISH_VOLUME",
        //"LIST_VOLUMES_PUBLISHED_NODES",
        // "LIST_VOLUMES",
        "GET_CAPACITY",
        "CREATE_DELETE_SNAPSHOT",
        // "LIST_SNAPSHOTS",
        "CLONE_VOLUME",
        //"PUBLISH_READONLY",
        "EXPAND_VOLUME",
      ];

      if (semver.satisfies(this.ctx.csiVersion, ">=1.3.0")) {
        options.service.controller.capabilities.rpc.push(
          //"VOLUME_CONDITION",
          // "GET_VOLUME"
        );
      }

      if (semver.satisfies(this.ctx.csiVersion, ">=1.5.0")) {
        options.service.controller.capabilities.rpc.push(
          "SINGLE_NODE_MULTI_WRITER"
        );
      }
    }

    if (!("rpc" in options.service.node.capabilities)) {
      this.ctx.logger.debug("setting default node caps");
      options.service.node.capabilities.rpc = [
        //"UNKNOWN",
        "STAGE_UNSTAGE_VOLUME",
        "GET_VOLUME_STATS",
        "EXPAND_VOLUME",
        //"VOLUME_CONDITION",
      ];

      if (semver.satisfies(this.ctx.csiVersion, ">=1.3.0")) {
        //options.service.node.capabilities.rpc.push("VOLUME_CONDITION");
      }

      if (semver.satisfies(this.ctx.csiVersion, ">=1.5.0")) {
        options.service.node.capabilities.rpc.push("SINGLE_NODE_MULTI_WRITER");
        /**
         * This is for volumes that support a mount time gid such as smb or fat
         */
        //options.service.node.capabilities.rpc.push("VOLUME_MOUNT_GROUP"); // in k8s is sent in as the security context fsgroup
      }
    }
  }

  parseVolumeHandle(handle, prefix = volumeIdPrefix) {
    if (!handle.startsWith(prefix)) {
      throw new GrpcError(
        grpc.status.INVALID_ARGUMENT,
        `invalid volume handle: ${handle}: expected prefix ${prefix}`
      );
    }
    handle = handle.substring(prefix.length);
    return {
      connectionName: handle.substring(0, handle.indexOf('/')),
      realHandle: handle.substring(handle.indexOf('/') + 1),
    };
  }

  decorateVolumeHandle(connectionName, handle, prefix = volumeIdPrefix) {
    return prefix + connectionName + '/' + handle;
  }

  // returns real driver object
  // internally drivers are cached and deleted on timeout
  lookUpConnection(connectionName) {
    const configFolder = this.options.proxy.configFolder;
    const configPath = configFolder + '/' + connectionName + '.yaml';

    if (this.timeout == 0) {
      // when timeout is 0, force creating a new driver on each request
      return this.createDriverFromFile(configPath);
    }

    const driverPlaceholder = {
      connectionName: connectionName,
      fileTime: 0,
      driver: null,
    };
    const cachedDriver = this.ctx.registry.get(`controller:driver/connection=${connectionName}`, driverPlaceholder);
    if (cachedDriver.timer !== null) {
      clearTimeout(cachedDriver.timer);
      cachedDriver.timer = null;
    }
    if (this.enableCacheTimeout) {
      cachedDriver.timer = setTimeout(() => {
        this.ctx.logger.info("removing inactive connection: %s", connectionName);
        this.ctx.registry.delete(`controller:driver/connection=${connectionName}`);
        cachedDriver.timer = null;
      }, this.timeout);
    }

    const fileTime = this.getFileTime(configPath);
    if (cachedDriver.fileTime != fileTime) {
      this.ctx.logger.debug("connection version is old: file time %d != %d", cachedDriver.fileTime, fileTime);
      cachedDriver.fileTime = fileTime;
      this.ctx.logger.info("creating a new connection: %s", connectionName);
      cachedDriver.driver = this.createDriverFromFile(configPath);
    }
    return cachedDriver.driver;
  }

  getFileTime(path) {
    try {
      const configFileStats = fs.statSync(path);
      this.ctx.logger.debug("file time for '%s' is: %d", path, configFileStats.mtime);
      return configFileStats.mtime.getTime();
    } catch (e) {
      this.ctx.logger.error("fs.statSync failed: %s", e.toString());
      throw e;
    }
  }

  createDriverFromFile(configPath) {
    const fileOptions = this.createOptionsFromFile(configPath);
    const mergedOptions = structuredClone(this.options);
    _.merge(mergedOptions, fileOptions);
    return this.createRealDriver(mergedOptions);
  }

  createOptionsFromFile(configPath) {
    this.ctx.logger.debug("loading config: %s", configPath);
    try {
      return yaml.load(fs.readFileSync(configPath, "utf8"));
    } catch (e) {
      this.ctx.logger.error("failed parsing config file: %s", e.toString());
      throw e;
    }
  }

  validateDriverType(driver) {
    const unsupportedDrivers = [
      "zfs-local-ephemeral-inline",
      "objectivefs",
      "proxy",
    ];
    for (const prefix in unsupportedDrivers) {
      if (driver.startsWith(prefix)) {
        throw new GrpcError(
          grpc.status.INVALID_ARGUMENT,
          `proxy is not supported for driver: ${mergedOptions.driver}`
        );
      }
    }
  }

  createRealDriver(options) {
    this.validateDriverType(options.driver);
    const realContext = Object.assign({}, this.ctx);
    realContext.registry = new Registry();
    const realDriver = this.ctx.factory(realContext, options);
    if (realDriver.constructor.name == this.constructor.name) {
      throw new GrpcError(
        grpc.status.INVALID_ARGUMENT,
        `cyclic dependency: proxy on proxy`
      );
    }
    this.ctx.logger.debug("using driver %s", realDriver.constructor.name);
    return realDriver;
  }

  async checkAndRun(driver, methodName, call, defaultValue) {
    if (typeof driver[methodName] !== 'function') {
      if (defaultValue) return defaultValue;
      // UNIMPLEMENTED could possibly confuse CSI CO into thinking
      // that driver does not support methodName at all.
      // INVALID_ARGUMENT should allow CO to use methodName with other storage classes.
      throw new GrpcError(
        grpc.status.INVALID_ARGUMENT,
        `underlying driver does not support ` + methodName
      );
    }
    return await driver[methodName](call);
  }

  async controllerRunWrapper(methodName, call, defaultValue) {
    const volumeHandle = this.parseVolumeHandle(call.request.volume_id);
    const driver = this.lookUpConnection(volumeHandle.connectionName);
    call.request.volume_id = volumeHandle.realHandle;
    return await this.checkAndRun(driver, methodName, call, defaultValue);
  }

  // ===========================================
  //    Controller methods below
  // ===========================================

  async GetCapacity(call) {
    const parameters = call.request.parameters;
    if (!parameters.connection) {
      throw new GrpcError(
        grpc.status.INVALID_ARGUMENT,
        `connection missing from parameters`
      );
    }
    const connectionName = parameters.connection;
    const driver = this.lookUpConnection(connectionName);
    return await this.checkAndRun(driver, 'GetCapacity', call, {
      available_capacity: Number.MAX_SAFE_INTEGER,
    });
  }

  async CreateVolume(call) {
    const parameters = call.request.parameters;
    if (!parameters.connection) {
      throw new GrpcError(
        grpc.status.INVALID_ARGUMENT,
        `connection missing from parameters`
      );
    }
    const connectionName = parameters.connection;
    const driver = this.lookUpConnection(connectionName);

    switch (call.request.volume_content_source?.type) {
      case "snapshot": {
        const snapshotHandle = this.parseVolumeHandle(call.request.volume_content_source.snapshot.snapshot_id, snapshotIdPrefix);
        if (snapshotHandle.connectionName != connectionName) {
          throw new GrpcError(
            grpc.status.INVALID_ARGUMENT,
            `can not inflate snapshot from a different connection`
          );
        }
        call.request.volume_content_source.snapshot.snapshot_id = snapshotHandle.realHandle;
        break;
      }
      case "volume": {
        const volumeHandle = this.parseVolumeHandle(call.request.volume_content_source.volume.volume_id);
        if (volumeHandle.connectionName != connectionName) {
          throw new GrpcError(
            grpc.status.INVALID_ARGUMENT,
            `can not clone volume from a different connection`
          );
        }
        call.request.volume_content_source.volume.volume_id = volumeHandle.realHandle;
        break;
      }
      case undefined:
      case null:
        break;
      default:
        throw new GrpcError(
          grpc.status.INVALID_ARGUMENT,
          `unknown volume_content_source type: ${call.request.volume_content_source.type}`
        );
    }
    const result = await this.checkAndRun(driver, 'CreateVolume', call);
    result.volume.volume_id = this.decorateVolumeHandle(connectionName, result.volume.volume_id);
    return result;
  }

  async DeleteVolume(call) {
    return await this.controllerRunWrapper('DeleteVolume', call);
  }

  async ControllerGetVolume(call) {
    return await this.controllerRunWrapper('ControllerGetVolume', call);
  }

  async ControllerExpandVolume(call) {
    return await this.controllerRunWrapper('ControllerExpandVolume', call);
  }

  async CreateSnapshot(call) {
    const volumeHandle = this.parseVolumeHandle(call.request.source_volume_id);
    const driver = this.lookUpConnection(volumeHandle.connectionName);
    call.request.source_volume_id = volumeHandle.realHandle;
    const result = await this.checkAndRun(driver, 'CreateSnapshot', call);
    result.snapshot.source_volume_id = this.decorateVolumeHandle(connectionName, result.snapshot.source_volume_id);
    result.snapshot.snapshot_id = this.decorateVolumeHandle(connectionName, result.snapshot.snapshot_id, snapshotIdPrefix);
    return result;
  }

  async DeleteSnapshot(call) {
    const volumeHandle = this.parseVolumeHandle(call.request.snapshot_id, snapshotIdPrefix);
    const driver = this.lookUpConnection(volumeHandle.connectionName);
    call.request.snapshot_id = volumeHandle.realHandle;
    return await this.checkAndRun(driver, 'DeleteSnapshot', call);
  }

  async ValidateVolumeCapabilities(call) {
    return await this.controllerRunWrapper('ValidateVolumeCapabilities', call);
  }

  // ===========================================
  //    Node methods below
  // ===========================================
  //
  // Theoretically, controller setup with config files could be replicated in node deployment,
  // and node could create proper drivers for each call.
  // But it doesn't seem like node would benefit from this.
  // - CsiBaseDriver.NodeStageVolume calls this.assertCapabilities which should be run in the real driver
  //   but no driver-specific functions or options are used.
  //   So we can just create an empty driver with default options
  // - Other Node* methods don't use anything driver specific

  lookUpNodeDriver(call) {
    const driverType = call.request.volume_context.provisioner_driver;
    return this.ctx.registry.get(`node:driver/${driverType}`, () => {
      const driverOptions = structuredClone(this.options);
      driverOptions.driver = driverType;
      return this.createRealDriver(driverOptions);
    });
  }

  async NodeStageVolume(call) {
    const driver = this.lookUpNodeDriver(call);
    return await this.checkAndRun(driver, 'NodeStageVolume', call);
  }

  async NodeGetInfo(call) {
    const result = {
      node_id: this.nodeIdSerializer.serialize(),
      max_volumes_per_node: 0,
    };
    const topologyType = this.options.proxy.nodeTopology?.type ?? 'cluster';
    const prefix = this.options.proxy.nodeTopology?.prefix ?? TOPOLOGY_DEFAULT_PREFIX;
    switch (topologyType) {
      case 'cluster':
        result.accessible_topology = {
          segments: {
            [prefix + '/cluster']: 'local',
          },
        };
        break
      case 'node':
        result.accessible_topology = {
          segments: {
            [prefix + '/node']: NodeIdSerializer.getLocalNodeName(),
          },
        };
        break
      default:
        throw new GrpcError(
          grpc.status.INVALID_ARGUMENT,
          `proxy: unknown node topology type: ${topologyType}`
        );
    }
    return result;
  }
}

const nodeIdCode_NodeName = 'n';
const nodeIdCode_Hostname = 'h';
const nodeIdCode_ISCSI = 'i';
const nodeIdCode_NVMEOF = 'v';
class NodeIdSerializer {
  constructor(ctx, nodeIdConfig) {
    this.ctx = ctx;
    this.config = nodeIdConfig;
  }

  static getLocalNodeName() {
    if (!process.env.CSI_NODE_ID) {
      throw 'CSI_NODE_ID is required for proxy driver';
    }
    return process.env.CSI_NODE_ID;
  }
  static getLocalIqn() {
    const iqnPath = '/etc/iscsi/initiatorname.iscsi';
    const lines = fs.readFileSync(iqnPath, "utf8").split('\n');
    for (let line of lines) {
      line = line.replace(/#.*/, '').replace(/\s+$/, '');
      if (line == '') {
        continue;
      }
      const linePrefix = 'InitiatorName=';
      if (line.startsWith(linePrefix)) {
        const iqn = line.slice(linePrefix.length);
        return iqn;
      }
    }
    throw 'iqn is not found';
  }
  static getLocalNqn() {
    const nqnPath = '/etc/nvme/hostnqn';
    return fs.readFileSync(nqnPath, "utf8").replace(/\s+$/, '');
  }

  // returns { prefixName, suffix }
  findPrefix(value, prefixMap) {
    for (const prefixInfo of prefixMap) {
      if (value.startsWith(prefixInfo.prefix)) {
        return {
          prefixName: prefixInfo.shortName,
          suffix: value.split(prefixInfo.prefix.length),
        };
      }
    }
  }

  serializeByPrefix(code, value, prefixMap) {
    const prefixInfo = prefixMap.find(prefixInfo => value.startsWith(prefixInfo.prefix));
    if (!prefixInfo) {
      throw `node id: prefix is not found for value: ${value}`;
    }
    if (!prefixInfo.shortName.match(/^[0-9a-z]+$/)) {
      throw `prefix short name must be alphanumeric, invalid name: '${prefixInfo.shortName}'`;
    }
    const suffix = value.substring(prefixInfo.prefix.length);
    return code + prefixInfo.shortName + '=' + suffix;
  }

  deserializeFromPrefix(value, prefixMap, humanName) {
    const prefixName = value.substring(0, value.indexOf('='));
    const suffix = value.substring(value.indexOf('=') + 1);
    const prefixInfo = prefixMap.find(prefixInfo => prefixInfo.shortName === prefixName);
    if (!prefixInfo) {
      throw new GrpcError(
        grpc.status.INVALID_ARGUMENT,
        `unknown node prefix short name for ${humanName}: ${value}`
      );
    }
    return prefixInfo.prefix + suffix;
  }

  // returns a single string that incorporates node id components specified in config.parts
  serialize() {
    let result = '';
    if (this.config.parts?.nodeName ?? true) {
      result += '/' + nodeIdCode_NodeName + '=' + NodeIdSerializer.getLocalNodeName();
    }
    if (this.config.parts?.hostname ?? false) {
      result += '/' + nodeIdCode_Hostname + '=' + os.hostname();
    }
    if (this.config.parts?.iqn ?? false) {
      result += '/' + this.serializeByPrefix(nodeIdCode_ISCSI, NodeIdSerializer.getLocalIqn(), this.config.iqnPrefix);
    }
    if (this.config.parts?.nqn ?? false) {
      result += '/' + this.serializeByPrefix(nodeIdCode_NVMEOF, NodeIdSerializer.getLocalNqn(), this.config.nqnPrefix);
    }
    if (result === '') {
      throw 'node id can not be empty';
    }
    // remove starting /
    return result.slice(1);
  }

  // takes a string generated by NodeIdSerializer.serialize
  // returns an { nodeName, iqn, nqn } if they exist in nodeId
  deserialize(nodeId) {
    const result = {};
    for (const v in nodeId.split("/")) {
      switch (v[0]) {
        case nodeIdCode_NodeName:
          result.nodeName = v.substring(v.indexOf('=') + 1);
          continue;
        case nodeIdCode_Hostname:
          result.hostname = v.substring(v.indexOf('=') + 1);
          continue;
        case nodeIdCode_ISCSI:
          result.iqn = this.deserializeFromPrefix(v, this.config.iqnPrefix, 'iSCSI');
          continue;
        case nodeIdCode_NVMEOF:
          result.nqn = this.deserializeFromPrefix(v, this.config.nqnPrefix, 'NVMEoF');
          continue;
      }
    }
    return result;
  }
}

module.exports.CsiProxyDriver = CsiProxyDriver;
