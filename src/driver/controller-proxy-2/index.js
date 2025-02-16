const _ = require("lodash");
const semver = require("semver");
const { CsiBaseDriver } = require("../index");
const yaml = require("js-yaml");
const fs = require('fs');
const { Registry } = require("../../utils/registry");
const { GrpcError, grpc } = require("../../utils/grpc");

// ===========================================
//    Compatibility
// ===========================================
//
// PUBLISH_UNPUBLISH_VOLUME capability is not supported
// It would require coordination with NodeGetInfo to get proper node_id value.
// Currently no driver in the repo implements ControllerPublishVolume
// so support for publishing is a low priority task.
//
// If some methods are missing from underlying driver,
// proxy will throw UNIMPLEMENTED error.
// If this is not desired, some methods could return default value instead.
//
// Some drivers will not work properly
// zfs-local-ephemeral-inline is not supported
// - NodePublishVolume needs this.options
// - - Possible fix: Add whole config to node publish secret
// - NodeUnpublishVolume does not have context to get driver
// - - Possible fix: add driver to volume_id
// zfs-local-* is not supported
// - NodeGetInfo does not have context to get driver
// - - Possible fix: move NodeGetInfo into CsiBaseDriver
// local-hostpath is not supported
// - NodeGetInfo does not have context to get driver
// - - Possible fix: move NodeGetInfo into CsiBaseDriver
// objectivefs is not supported
// - NodeStageVolume needs this.options in getDefaultObjectiveFSInstance, used only in NodeStageVolume
// - - Possible fix: Add whole config to node stage secret
// - - Possible fix: add public pool data into volume attributes, possibly add private data into a secret

// ===========================================
//    Features
// ===========================================
//
// There are some missing features:
// - GetCapacity is not possible, there is no concept of unified storage for proxy
// - ListVolumes is not possible, there is no concept of unified storage for proxy
// - ListSnapshots is not implemented
// - - TODO add snapshot secret with connection name into storage class

// ===========================================
//    Volume cloning and snapshots
// ===========================================
//
// Cloning works without any adjustments when both volumes use the same connection.
// If the connection is different:
// - TODO: Same driver, same server
// - - Just need to get proper source location in the CreateVolume
// - TODO: Same driver, different servers
// - - It's up to driver to add support
// - - Example: zfs send-receive
// - - Example: file copy between nfs servers
// - Different drivers: block <-> file: unlikely to be practical, if even possible
// - Different drivers: same filesystem type
// - - Drivers should implement generic export and import functions
// - - For example: TrueNas -> generic-zfs can theoretically be possible via zfs send
// - - For example: nfs -> nfs can theoretically be possible via file copy
// - - How to coordinate different drivers?

// TODO support VOLUME_MOUNT_GROUP for SMB?
// TODO support mapping from generic node_id to proper iqn/nqn in case volume publish is needed
//      option: set node_id to node_name,iqn,nqn
//              [+] no configuration required
//              [-] node_id length limit can be exceeded with long iqn, nqn: 400+ > 256
//      option: set node_id to node_name
//              add config option maps: node_name -> iqn, node_name -> nqn
//              replace node_id before calling ControllerPublishVolume, just like we do with volume_id field
//              [-] this can be hard to maintain when nodes are added or reconfigured

// volume_id format:   v2:server-entry/original-handle
// snapshot_id format: v2:server-entry/original-handle
// 'v2' - fixed prefix
// server-entry - connection name
//                corresponds to `{this.options.proxy.configFolder}/{connectionName}.yaml`
// original-handle - handle created by the real driver

// a great discussion of difficulties with proxy driver can be found here:
// https://github.com/container-storage-interface/spec/issues/370
class CsiProxy2Driver extends CsiBaseDriver {
  constructor(ctx, options) {
    super(...arguments);
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
        //"VOLUME_ACCESSIBILITY_CONSTRAINTS"
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
        // "GET_CAPACITY",
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

  parseVolumeHandle(handle) {
    if (!handle.startsWith('v2:')) {
      throw 'invalid volume handle: ' + handle;
    }
    handle = handle.substring('v2:'.length);
    return {
      connectionName: handle.substring(0, handle.indexOf('/')),
      realHandle: handle.substring(handle.indexOf('/') + 1),
    };
  }

  decorateVolumeHandle(connectionName, handle) {
    return 'v2:' + connectionName + '/' + handle;
  }

  // returns real driver object
  lookUpConnection(connectionName) {
    const configFolder = this.options.proxy.configFolder;
    const configPath = configFolder + '/' + connectionName + '.yaml';

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
    // 1 hour timeout
    const oneHourInMs = 1000 * 60 * 60;
    cachedDriver.timer = setTimeout(() => {
      this.ctx.logger.info("removing inactive connection: %v", connectionName);
      this.ctx.registry.delete(`controller:driver/connection=${connectionName}`);
      cachedDriver.timer = null;
    }, oneHourInMs);

    const fileTime = this.getFileTime(configPath);
    if (cachedDriver.fileTime != fileTime) {
      cachedDriver.fileTime = fileTime;
      this.ctx.logger.info("creating a new connection: %v", connectionName);
      cachedDriver.driver = this.createDriverFromFile(configPath);
    }
    return cachedDriver.driver;
  }

  getFileTime(path) {
    try {
      const configFileStats = fs.statSync(path);
      this.ctx.logger.debug("file time %s %v", path, configFileStats.mtime);
      return configFileStats.mtime;
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
      "zfs-local-",
      "local-hostpath",
      "objectivefs",
      "proxy",
    ];
    for (const prefix in unsupportedDrivers) {
      if (driver.startsWith(prefix)) {
        throw "proxy is not supported for driver: " + mergedOptions.driver;
      }
    }
  }

  createRealDriver(options) {
    this.validateDriverType(options.driver);
    const realContext = Object.assign({}, this.ctx);
    realContext.registry = new Registry();
    const realDriver = this.ctx.factory(realContext, options);
    if (realDriver.constructor.name == this.constructor.name) {
      throw "cyclic dependency: proxy on proxy";
    }
    this.ctx.logger.debug("using driver %s", realDriver.constructor.name);
    return realDriver;
  }

  async checkAndRun(driver, methodName, call, defaultValue) {
    if(typeof driver[methodName] !== 'function') {
      if (defaultValue) return defaultValue;
      throw new GrpcError(
        grpc.status.UNIMPLEMENTED,
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

  async CreateVolume(call) {
    const parameters = call.request.parameters;
    if (!parameters.connection) {
      throw 'connection missing from parameters';
    }
    const connectionName = parameters.connection;
    const driver = this.lookUpConnection(connectionName);

    switch (call.request.volume_content_source?.type) {
      case "snapshot": {
        const snapshotHandle = this.parseVolumeHandle(call.request.volume_content_source.snapshot.snapshot_id);
        if (snapshotHandle.connectionName != connectionName) {
          throw "could not inflate snapshot from a different connection";
        }
        call.request.volume_content_source.snapshot.snapshot_id = snapshotHandle.realHandle;
        break;
      }
      case "volume": {
        const volumeHandle = this.parseVolumeHandle(call.request.volume_content_source.volume.volume_id);
        if (volumeHandle.connectionName != connectionName) {
          throw "could not clone volume from a different connection";
        }
        call.request.volume_content_source.volume.volume_id = volumeHandle.realHandle;
        break;
      }
      case undefined:
      case null:
        break;
      default:
        throw 'unknown volume_content_source type: ' + call.request.volume_content_source.type;
    }
    const result = await this.checkAndRun(driver, 'CreateVolume', call);
    this.ctx.logger.debug("CreateVolume result " + result);
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
    result.snapshot.snapshot_id = this.decorateVolumeHandle(connectionName, result.snapshot.snapshot_id);
    return result;
  }

  async DeleteSnapshot(call) {
    const volumeHandle = this.parseVolumeHandle(call.request.snapshot_id);
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
  //
  // There are a few exceptions:
  // - zfs-local-ephemeral-inline: could be fixed with local config, but I see this as a low priority task
  // - zfs-local-*: doesn't depend on config
  // - local-hostpath: doesn't depend on config
  // - objectivefs: could be fixed with local config or without it
  lookUpNodeDriver(call) {
    const driverName = call.request.volume_context.provisioner_driver;
    return this.ctx.registry.get(`node:driver/${driverName}`, () => {
      const driverOptions = structuredClone(this.options);
      driverOptions.driver = call.request.volume_context.provisioner_driver;
      return this.createRealDriver(driverOptions);
    });
  }

  async NodeStageVolume(call) {
    const driver = this.lookUpNodeDriver(call);
    return await this.checkAndRun(driver, 'NodeStageVolume', call);
  }
}

module.exports.CsiProxy2Driver = CsiProxy2Driver;
