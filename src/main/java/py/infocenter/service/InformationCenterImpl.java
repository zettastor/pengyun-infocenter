/*
 * Copyright (c) 2022. PengYunNetWork
 *
 * This program is free software: you can use, redistribute, and/or modify it
 * under the terms of the GNU Affero General Public License, version 3 or later ("AGPL"),
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 *  You should have received a copy of the GNU Affero General Public License along with
 *  this program. If not, see <http://www.gnu.org/licenses/>.
 */

package py.infocenter.service;

import static com.google.common.collect.HashMultimap.create;
import static py.RequestResponseHelper.buildDiskSmartInfoFrom;
import static py.RequestResponseHelper.buildScsiDeviceStatusType;
import static py.RequestResponseHelper.buildSensorInfoFrom;
import static py.RequestResponseHelper.buildThriftDriverMetadataFrom;
import static py.RequestResponseHelper.buildVolumeSourceTypeFrom;
import static py.RequestResponseHelper.convertFromSegmentUnitTypeThrift;
import static py.common.Constants.DEFAULT_PASSWORD;
import static py.common.Constants.SUPERADMIN_ACCOUNT_ID;
import static py.icshare.InstanceMetadata.DatanodeStatus.OK;
import static py.icshare.InstanceMetadata.DatanodeStatus.UNKNOWN;
import static py.informationcenter.Utils.MOVE_VOLUME_APPEND_STRING_FOR_ORIGINAL_VOLUME;
import static py.volume.VolumeInAction.CREATING;
import static py.volume.VolumeInAction.DELETING;
import static py.volume.VolumeInAction.EXTENDING;
import static py.volume.VolumeInAction.RECYCLING;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import edu.emory.mathcs.backport.java.util.Collections;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.Validate;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.AbstractConfigurationServer;
import py.RequestResponseHelper;
import py.app.NetworkConfiguration;
import py.app.context.AppContext;
import py.app.thrift.ThriftProcessorFactory;
import py.archive.ArchiveMetadata;
import py.archive.ArchiveStatus;
import py.archive.RawArchiveMetadata;
import py.archive.segment.SegId;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.archive.segment.SegmentUnitStatusConflictCause;
import py.archive.segment.SegmentVersion;
import py.client.thrift.GenericThriftClientFactory;
import py.common.Constants;
import py.common.PyService;
import py.common.RequestIdBuilder;
import py.common.VolumeMetadataJsonParser;
import py.common.struct.EndPoint;
import py.common.struct.EndPointParser;
import py.common.struct.Pair;
import py.driver.DriverContainerCandidate;
import py.driver.DriverMetadata;
import py.driver.DriverStateEvent;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.driver.ScsiDriverDescription;
import py.drivercontainer.client.CoordinatorClientFactory;
import py.drivercontainer.client.DriverContainerClientFactory;
import py.drivercontainer.client.DriverContainerServiceBlockingClientWrapper;
import py.exception.EndPointNotFoundException;
import py.exception.GenericThriftClientFactoryException;
import py.exception.SegmentUnitStatusConflictExeption;
import py.exception.TooManyEndPointFoundException;
import py.exception.VolumeExtendFailedException;
import py.icshare.AccessRuleInformation;
import py.icshare.AccessRuleStatusBindingVolume;
import py.icshare.AccountMetadata;
import py.icshare.CapacityRecord;
import py.icshare.CapacityRecordStore;
import py.icshare.DiskInfo;
import py.icshare.DiskSmartInfo;
import py.icshare.Domain;
import py.icshare.DomainStore;
import py.icshare.DriverClientInformation;
import py.icshare.DriverClientKey;
import py.icshare.DriverKey;
import py.icshare.DriverKeyForScsi;
import py.icshare.InstanceMaintenanceDbStore;
import py.icshare.InstanceMaintenanceInformation;
import py.icshare.InstanceMetadata;
import py.icshare.Operation;
import py.icshare.OperationStatus;
import py.icshare.OperationType;
import py.icshare.RecoverDbSentryStore;
import py.icshare.ScsiClientStore;
import py.icshare.ScsiDriverMetadata;
import py.icshare.SensorInfo;
import py.icshare.ServerNode;
import py.icshare.TargetType;
import py.icshare.UmountDriverRequest;
import py.icshare.Volume2AccessRuleRelationship;
import py.icshare.VolumeAccessRule;
import py.icshare.VolumeCreationRequest;
import py.icshare.VolumeDeleteDelayInformation;
import py.icshare.VolumeRecycleInformation;
import py.icshare.VolumeRuleRelationshipInformation;
import py.icshare.authorization.AccountStore;
import py.icshare.authorization.ApiStore;
import py.icshare.authorization.ApiToAuthorize;
import py.icshare.authorization.PyResource;
import py.icshare.authorization.ResourceStore;
import py.icshare.authorization.Role;
import py.icshare.authorization.RoleStore;
import py.icshare.exception.VolumeNotFoundException;
import py.icshare.iscsiaccessrule.Iscsi2AccessRuleRelationship;
import py.icshare.iscsiaccessrule.IscsiAccessRule;
import py.icshare.iscsiaccessrule.IscsiAccessRuleInformation;
import py.icshare.iscsiaccessrule.IscsiRuleRelationshipInformation;
import py.icshare.qos.MigrationRule;
import py.icshare.qos.MigrationRuleInformation;
import py.icshare.qos.MigrationRuleStore;
import py.icshare.qos.RebalanceRule;
import py.icshare.qos.RebalanceRuleInformation;
import py.icshare.qos.RebalanceRuleStore;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.InfoCenterConfiguration;
import py.infocenter.InformationCenterAppEngine;
import py.infocenter.authorization.PySecurityManager;
import py.infocenter.client.VolumeMetadataAndDrivers;
import py.infocenter.common.InfoCenterConstants;
import py.infocenter.create.volume.CreateSegmentUnitsRequest;
import py.infocenter.create.volume.CreateVolumeManager;
import py.infocenter.dbmanager.BackupDbManager;
import py.infocenter.dbmanager.BackupDbManagerImpl;
import py.infocenter.driver.client.manger.DriverClientManger;
import py.infocenter.instance.manger.InstanceIncludeVolumeInfoManger;
import py.infocenter.instance.manger.InstanceToVolumeInfo;
import py.infocenter.instance.manger.InstanceVolumeInEquilibriumManger;
import py.infocenter.instance.manger.VolumeInformationManger;
import py.infocenter.job.TaskType;
import py.infocenter.rebalance.ReserveSegUnitResult;
import py.infocenter.rebalance.ReserveSegUnitsInfo;
import py.infocenter.rebalance.SegmentUnitsDistributionManager;
import py.infocenter.rebalance.exception.NoNeedToRebalance;
import py.infocenter.rebalance.old.RebalanceTaskExecutor;
import py.infocenter.rebalance.struct.BaseRebalanceTask;
import py.infocenter.rebalance.struct.SendRebalanceTask;
import py.infocenter.rebalance.struct.SimulateSegmentUnit;
import py.infocenter.reportvolume.AllDriverClearAccessRulesManager;
import py.infocenter.reportvolume.ReportVolumeManager;
import py.infocenter.service.selection.ComparisonSelectionStrategy;
import py.infocenter.service.selection.DriverContainerSelectionStrategy;
import py.infocenter.service.selection.SelectionStrategy;
import py.infocenter.store.AccessRuleStore;
import py.infocenter.store.DiskInfoStore;
import py.infocenter.store.DriverStore;
import py.infocenter.store.IscsiAccessRuleStore;
import py.infocenter.store.IscsiRuleRelationshipStore;
import py.infocenter.store.OrphanVolumeStore;
import py.infocenter.store.ScsiDriverStore;
import py.infocenter.store.SegmentUnitTimeoutStore;
import py.infocenter.store.ServerNodeStore;
import py.infocenter.store.StorageStore;
import py.infocenter.store.TaskRequestInfo;
import py.infocenter.store.TaskStore;
import py.infocenter.store.VolumeDelayStore;
import py.infocenter.store.VolumeRecycleStore;
import py.infocenter.store.VolumeRuleRelationshipStore;
import py.infocenter.store.VolumeStatusTransitionStore;
import py.infocenter.store.VolumeStore;
import py.infocenter.store.control.DeleteVolumeRequest;
import py.infocenter.store.control.OperationStore;
import py.infocenter.store.control.VolumeJobStoreDb;
import py.infocenter.volume.recycle.VolumeRecycleManager;
import py.infocenter.worker.CreateSegmentUnitWorkThread;
import py.infocenter.worker.GetRebalanceTaskSweeper;
import py.infocenter.worker.StoragePoolSpaceCalculator;
import py.informationcenter.AccessPermissionType;
import py.informationcenter.AccessRuleStatus;
import py.informationcenter.LaunchDriverRequest;
import py.informationcenter.PoolInfo;
import py.informationcenter.ScsiClient;
import py.informationcenter.ScsiClientInfo;
import py.informationcenter.Status;
import py.informationcenter.StoragePool;
import py.informationcenter.StoragePoolLevel;
import py.informationcenter.StoragePoolStore;
import py.informationcenter.StoragePoolStrategy;
import py.instance.DcType;
import py.instance.Group;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.instance.PortType;
import py.io.qos.IoLimitation;
import py.io.qos.IoLimitationStatus;
import py.io.qos.IoLimitationStore;
import py.io.qos.MigrationRuleStatus;
import py.io.qos.MigrationStrategy;
import py.membership.SegmentForm;
import py.membership.SegmentMembership;
import py.monitor.common.CounterName;
import py.monitor.common.DiskInfoLightStatus;
import py.monitor.common.OperationName;
import py.monitor.common.UserDefineName;
import py.querylog.eventdatautil.EventDataWorker;
import py.rebalance.RebalanceTask;
import py.systemdaemon.common.NodeInfo;
import py.thrift.coordinator.service.AddOrModifyLimitationRequest;
import py.thrift.coordinator.service.Coordinator;
import py.thrift.coordinator.service.DeleteLimitationRequest;
import py.thrift.coordinator.service.UpdateVolumeOnExtendingRequest;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.icshare.AddOrModifyIoLimitRequest;
import py.thrift.icshare.AddOrModifyIoLimitResponse;
import py.thrift.icshare.CancelDeleteVolumeDelayRequest;
import py.thrift.icshare.CancelDeleteVolumeDelayResponse;
import py.thrift.icshare.Capacity;
import py.thrift.icshare.ChangeLimitTypeRequest;
import py.thrift.icshare.ChangeLimitTypeResponse;
import py.thrift.icshare.ClientTotal;
import py.thrift.icshare.CreateAccessRuleOnNewVolumeRequest;
import py.thrift.icshare.CreateAccessRuleOnNewVolumeResponse;
import py.thrift.icshare.CreateVolumeRequest;
import py.thrift.icshare.CreateVolumeResponse;
import py.thrift.icshare.DeleteAccessRuleOnOldVolumeRequest;
import py.thrift.icshare.DeleteAccessRuleOnOldVolumeResponse;
import py.thrift.icshare.DeleteIoLimitRequest;
import py.thrift.icshare.DeleteIoLimitResponse;
import py.thrift.icshare.DeleteVolumeDelayRequest;
import py.thrift.icshare.DeleteVolumeDelayResponse;
import py.thrift.icshare.DeleteVolumeResponse;
import py.thrift.icshare.DiskStatistics;
import py.thrift.icshare.GetArchiveRequestThrift;
import py.thrift.icshare.GetArchiveResponseThrift;
import py.thrift.icshare.GetArchivesRequestThrift;
import py.thrift.icshare.GetArchivesResponseThrift;
import py.thrift.icshare.GetDashboardInfoRequest;
import py.thrift.icshare.GetDashboardInfoResponse;
import py.thrift.icshare.GetDeleteVolumeDelayRequest;
import py.thrift.icshare.GetDeleteVolumeDelayResponse;
import py.thrift.icshare.GetDriversRequestThrift;
import py.thrift.icshare.GetDriversResponseThrift;
import py.thrift.icshare.GetLimitsRequest;
import py.thrift.icshare.GetLimitsResponse;
import py.thrift.icshare.GetSegmentListRequest;
import py.thrift.icshare.GetSegmentListResponse;
import py.thrift.icshare.GetSegmentRequest;
import py.thrift.icshare.GetSegmentResponse;
import py.thrift.icshare.GetVolumeRequest;
import py.thrift.icshare.GetVolumeResponse;
import py.thrift.icshare.InstanceStatusStatistics;
import py.thrift.icshare.ListAllDriversRequest;
import py.thrift.icshare.ListAllDriversResponse;
import py.thrift.icshare.ListArchivesAfterFilterRequestThrift;
import py.thrift.icshare.ListArchivesAfterFilterResponseThrift;
import py.thrift.icshare.ListArchivesRequestThrift;
import py.thrift.icshare.ListArchivesResponseThrift;
import py.thrift.icshare.ListDriverClientInfoRequest;
import py.thrift.icshare.ListDriverClientInfoResponse;
import py.thrift.icshare.ListRecycleVolumeInfoRequest;
import py.thrift.icshare.ListRecycleVolumeInfoResponse;
import py.thrift.icshare.ListScsiDriverMetadataRequest;
import py.thrift.icshare.ListScsiDriverMetadataResponse;
import py.thrift.icshare.ListVolumesRequest;
import py.thrift.icshare.ListVolumesResponse;
import py.thrift.icshare.MoveVolumeToRecycleRequest;
import py.thrift.icshare.MoveVolumeToRecycleResponse;
import py.thrift.icshare.OrphanVolumeRequest;
import py.thrift.icshare.OrphanVolumeResponse;
import py.thrift.icshare.PoolStatistics;
import py.thrift.icshare.RecycleVolumeRequest;
import py.thrift.icshare.RecycleVolumeResponse;
import py.thrift.icshare.RecycleVolumeToNormalRequest;
import py.thrift.icshare.RecycleVolumeToNormalResponse;
import py.thrift.icshare.ReportDriverMetadataRequest;
import py.thrift.icshare.ReportDriverMetadataResponse;
import py.thrift.icshare.ReportScsiDriverMetadataRequest;
import py.thrift.icshare.ReportScsiDriverMetadataResponse;
import py.thrift.icshare.ScsiClientDescriptionThrift;
import py.thrift.icshare.ScsiClientInfoThrift;
import py.thrift.icshare.ScsiClientStatusThrift;
import py.thrift.icshare.ScsiDeviceInfoThrift;
import py.thrift.icshare.ServerNodeStatistics;
import py.thrift.icshare.StartDeleteVolumeDelayRequest;
import py.thrift.icshare.StartDeleteVolumeDelayResponse;
import py.thrift.icshare.StopDeleteVolumeDelayRequest;
import py.thrift.icshare.StopDeleteVolumeDelayResponse;
import py.thrift.icshare.UpdateVolumeDescriptionRequest;
import py.thrift.icshare.UpdateVolumeDescriptionResponse;
import py.thrift.icshare.VolumeCounts;
import py.thrift.infocenter.service.AssignRolesRequest;
import py.thrift.infocenter.service.AssignRolesResponse;
import py.thrift.infocenter.service.CancelMaintenanceRequest;
import py.thrift.infocenter.service.CancelMaintenanceResponse;
import py.thrift.infocenter.service.ChangeVolumeStatusFromFixToUnavailableRequest;
import py.thrift.infocenter.service.ChangeVolumeStatusFromFixToUnavailableResponse;
import py.thrift.infocenter.service.CheckVolumeIsReadOnlyRequest;
import py.thrift.infocenter.service.CheckVolumeIsReadOnlyResponse;
import py.thrift.infocenter.service.CreateRoleNameExistedExceptionThrift;
import py.thrift.infocenter.service.CreateRoleRequest;
import py.thrift.infocenter.service.CreateRoleResponse;
import py.thrift.infocenter.service.CreateScsiClientRequest;
import py.thrift.infocenter.service.CreateScsiClientResponse;
import py.thrift.infocenter.service.CreateSegmentUnitRequest;
import py.thrift.infocenter.service.CreateSegmentUnitResponse;
import py.thrift.infocenter.service.CreateSegmentsResponse;
import py.thrift.infocenter.service.DeleteRolesRequest;
import py.thrift.infocenter.service.DeleteRolesResponse;
import py.thrift.infocenter.service.DeleteScsiClientRequest;
import py.thrift.infocenter.service.DeleteScsiClientResponse;
import py.thrift.infocenter.service.ExtendVolumeRequest;
import py.thrift.infocenter.service.ExtendVolumeResponse;
import py.thrift.infocenter.service.FailedToTellDriverAboutAccessRulesExceptionThrift;
import py.thrift.infocenter.service.GetSegmentSizeResponse;
import py.thrift.infocenter.service.GetServerNodeByIpRequest;
import py.thrift.infocenter.service.GetServerNodeByIpResponse;
import py.thrift.infocenter.service.InformationCenter;
import py.thrift.infocenter.service.InstanceMaintainRequest;
import py.thrift.infocenter.service.InstanceMaintainResponse;
import py.thrift.infocenter.service.InstanceMaintenanceThrift;
import py.thrift.infocenter.service.ListApisRequest;
import py.thrift.infocenter.service.ListApisResponse;
import py.thrift.infocenter.service.ListInstanceMaintenancesRequest;
import py.thrift.infocenter.service.ListInstanceMaintenancesResponse;
import py.thrift.infocenter.service.ListRolesRequest;
import py.thrift.infocenter.service.ListRolesResponse;
import py.thrift.infocenter.service.ListScsiClientRequest;
import py.thrift.infocenter.service.ListScsiClientResponse;
import py.thrift.infocenter.service.LoadVolumeRequest;
import py.thrift.infocenter.service.LoadVolumeResponse;
import py.thrift.infocenter.service.LogoutRequest;
import py.thrift.infocenter.service.LogoutResponse;
import py.thrift.infocenter.service.MarkVolumesReadWriteRequest;
import py.thrift.infocenter.service.MarkVolumesReadWriteResponse;
import py.thrift.infocenter.service.NoNeedToRebalanceThrift;
import py.thrift.infocenter.service.PingPeriodicallyRequest;
import py.thrift.infocenter.service.PingPeriodicallyResponse;
import py.thrift.infocenter.service.RecoverDatabaseResponse;
import py.thrift.infocenter.service.ReportArchivesRequest;
import py.thrift.infocenter.service.ReportArchivesResponse;
import py.thrift.infocenter.service.ReportJustCreatedSegmentUnitRequest;
import py.thrift.infocenter.service.ReportJustCreatedSegmentUnitResponse;
import py.thrift.infocenter.service.ReportSegmentUnitCloneFailRequest;
import py.thrift.infocenter.service.ReportSegmentUnitCloneFailResponse;
import py.thrift.infocenter.service.ReportSegmentUnitRecycleFailRequest;
import py.thrift.infocenter.service.ReportSegmentUnitRecycleFailResponse;
import py.thrift.infocenter.service.ReportSegmentUnitsMetadataRequest;
import py.thrift.infocenter.service.ReportSegmentUnitsMetadataResponse;
import py.thrift.infocenter.service.ReportVolumeInfoRequest;
import py.thrift.infocenter.service.ReportVolumeInfoResponse;
import py.thrift.infocenter.service.RetrieveOneRebalanceTaskRequest;
import py.thrift.infocenter.service.RetrieveOneRebalanceTaskResponse;
import py.thrift.infocenter.service.SaveOperationLogsToCsvRequest;
import py.thrift.infocenter.service.SaveOperationLogsToCsvResponse;
import py.thrift.infocenter.service.SegUnitConflictThrift;
import py.thrift.infocenter.service.UpdateRoleRequest;
import py.thrift.infocenter.service.UpdateRoleResponse;
import py.thrift.infocenter.service.UpdateVolumeLayoutRequest;
import py.thrift.infocenter.service.UpdateVolumeLayoutResponse;
import py.thrift.monitorserver.service.MonitorServer;
import py.thrift.monitorserver.service.ReportMaintenanceModeInfoRequest;
import py.thrift.monitorserver.service.ReportMaintenanceModeInfoResponse;
import py.thrift.share.AccessDeniedExceptionThrift;
import py.thrift.share.AccessPermissionTypeThrift;
import py.thrift.share.AccessRuleNotAppliedThrift;
import py.thrift.share.AccessRuleNotFoundThrift;
import py.thrift.share.AccessRuleStatusThrift;
import py.thrift.share.AccessRuleUnderOperationThrift;
import py.thrift.share.AccountAlreadyExistsExceptionThrift;
import py.thrift.share.AccountMetadataThrift;
import py.thrift.share.AccountNotFoundExceptionThrift;
import py.thrift.share.AccountTypeThrift;
import py.thrift.share.AddRebalanceRuleRequest;
import py.thrift.share.AlreadyExistStaticLimitationExceptionThrift;
import py.thrift.share.ApiToAuthorizeThrift;
import py.thrift.share.ApplyFailedDueToConflictExceptionThrift;
import py.thrift.share.ApplyFailedDueToVolumeIsReadOnlyExceptionThrift;
import py.thrift.share.ApplyIoLimitationsRequest;
import py.thrift.share.ApplyIoLimitationsResponse;
import py.thrift.share.ApplyIscsiAccessRuleOnIscsisRequest;
import py.thrift.share.ApplyIscsiAccessRuleOnIscsisResponse;
import py.thrift.share.ApplyIscsiAccessRulesRequest;
import py.thrift.share.ApplyIscsiAccessRulesResponse;
import py.thrift.share.ApplyMigrationRulesRequest;
import py.thrift.share.ApplyMigrationRulesResponse;
import py.thrift.share.ApplyRebalanceRuleRequest;
import py.thrift.share.ApplyVolumeAccessRuleExceptionThrift;
import py.thrift.share.ApplyVolumeAccessRuleOnVolumesRequest;
import py.thrift.share.ApplyVolumeAccessRuleOnVolumesResponse;
import py.thrift.share.ApplyVolumeAccessRulesRequest;
import py.thrift.share.ApplyVolumeAccessRulesResponse;
import py.thrift.share.ArbiterMigrateThrift;
import py.thrift.share.ArchiveIsUsingExceptionThrift;
import py.thrift.share.ArchiveManagerNotSupportExceptionThrift;
import py.thrift.share.ArchiveMetadataThrift;
import py.thrift.share.ArchiveNotFoundExceptionThrift;
import py.thrift.share.ArchiveNotFreeToUseExceptionThrift;
import py.thrift.share.ArchiveStatusThrift;
import py.thrift.share.AssignResourcesRequest;
import py.thrift.share.AssignResourcesResponse;
import py.thrift.share.AuthenticateAccountRequest;
import py.thrift.share.AuthenticateAccountResponse;
import py.thrift.share.AuthenticationFailedExceptionThrift;
import py.thrift.share.CanNotGetPydDriverExceptionThrift;
import py.thrift.share.CancelDriversRulesRequest;
import py.thrift.share.CancelDriversRulesResponse;
import py.thrift.share.CancelIoLimitationsRequest;
import py.thrift.share.CancelIoLimitationsResponse;
import py.thrift.share.CancelIscsiAccessRuleAllAppliedRequest;
import py.thrift.share.CancelIscsiAccessRuleAllAppliedResponse;
import py.thrift.share.CancelIscsiAccessRulesRequest;
import py.thrift.share.CancelIscsiAccessRulesResponse;
import py.thrift.share.CancelMigrationRulesRequest;
import py.thrift.share.CancelMigrationRulesResponse;
import py.thrift.share.CancelVolAccessRuleAllAppliedRequest;
import py.thrift.share.CancelVolAccessRuleAllAppliedResponse;
import py.thrift.share.CancelVolumeAccessRulesRequest;
import py.thrift.share.CancelVolumeAccessRulesResponse;
import py.thrift.share.ChangeDriverBoundVolumeRequest;
import py.thrift.share.ChangeDriverBoundVolumeResponse;
import py.thrift.share.ChapSameUserPasswdErrorThrift;
import py.thrift.share.CheckSecondaryInactiveThresholdModeThrift;
import py.thrift.share.CheckSecondaryInactiveThresholdThrift;
import py.thrift.share.CleanOperationInfoRequest;
import py.thrift.share.CleanOperationInfoResponse;
import py.thrift.share.ConfirmFixVolumeRequestThrift;
import py.thrift.share.ConfirmFixVolumeResponse;
import py.thrift.share.ConfirmFixVolumeResponseThrift;
import py.thrift.share.ConnectPydDeviceOperationExceptionThrift;
import py.thrift.share.CreateAccountRequest;
import py.thrift.share.CreateAccountResponse;
import py.thrift.share.CreateBackstoresOperationExceptionThrift;
import py.thrift.share.CreateDefaultDomainAndStoragePoolRequestThrift;
import py.thrift.share.CreateDefaultDomainAndStoragePoolResponseThrift;
import py.thrift.share.CreateDomainRequest;
import py.thrift.share.CreateDomainResponse;
import py.thrift.share.CreateIoLimitationsRequest;
import py.thrift.share.CreateIoLimitationsResponse;
import py.thrift.share.CreateIscsiAccessRulesRequest;
import py.thrift.share.CreateIscsiAccessRulesResponse;
import py.thrift.share.CreateLoopbackLunsOperationExceptionThrift;
import py.thrift.share.CreateLoopbackOperationExceptionThrift;
import py.thrift.share.CreateSegmentUnitInfo;
import py.thrift.share.CreateStoragePoolRequestThrift;
import py.thrift.share.CreateStoragePoolResponseThrift;
import py.thrift.share.CreateVolumeAccessRulesExceptionThrift;
import py.thrift.share.CreateVolumeAccessRulesRequest;
import py.thrift.share.CreateVolumeAccessRulesResponse;
import py.thrift.share.CrudBuiltInRoleExceptionThrift;
import py.thrift.share.CrudSuperAdminAccountExceptionThrift;
import py.thrift.share.DatanodeIsUsingExceptionThrift;
import py.thrift.share.DatanodeNotFoundExceptionThrift;
import py.thrift.share.DatanodeNotFreeToUseExceptionThrift;
import py.thrift.share.DatanodeStatusThrift;
import py.thrift.share.DeleteAccountsRequest;
import py.thrift.share.DeleteAccountsResponse;
import py.thrift.share.DeleteDomainRequest;
import py.thrift.share.DeleteDomainResponse;
import py.thrift.share.DeleteIoLimitationsRequest;
import py.thrift.share.DeleteIoLimitationsResponse;
import py.thrift.share.DeleteIscsiAccessRulesRequest;
import py.thrift.share.DeleteIscsiAccessRulesResponse;
import py.thrift.share.DeleteLoginAccountExceptionThrift;
import py.thrift.share.DeleteRebalanceRuleRequest;
import py.thrift.share.DeleteRebalanceRuleResponse;
import py.thrift.share.DeleteRoleExceptionThrift;
import py.thrift.share.DeleteServerNodesRequestThrift;
import py.thrift.share.DeleteServerNodesResponseThrift;
import py.thrift.share.DeleteStoragePoolRequestThrift;
import py.thrift.share.DeleteStoragePoolResponseThrift;
import py.thrift.share.DeleteVolumeAccessRulesRequest;
import py.thrift.share.DeleteVolumeAccessRulesResponse;
import py.thrift.share.DiskHasBeenOfflineThrift;
import py.thrift.share.DiskHasBeenOnlineThrift;
import py.thrift.share.DiskIsBusyThrift;
import py.thrift.share.DiskNotBrokenThrift;
import py.thrift.share.DiskNotFoundExceptionThrift;
import py.thrift.share.DiskNotMismatchConfigThrift;
import py.thrift.share.DiskSizeCanNotSupportArchiveTypesThrift;
import py.thrift.share.DiskSmartInfoThrift;
import py.thrift.share.DomainExistedExceptionThrift;
import py.thrift.share.DomainIsDeletingExceptionThrift;
import py.thrift.share.DomainNameExistedExceptionThrift;
import py.thrift.share.DomainNotExistedExceptionThrift;
import py.thrift.share.DriverAmountAndHostNotFitThrift;
import py.thrift.share.DriverClientInfoThrift;
import py.thrift.share.DriverContainerIsIncExceptionThrift;
import py.thrift.share.DriverHostCannotUseThrift;
import py.thrift.share.DriverIpTargetThrift;
import py.thrift.share.DriverIsLaunchingExceptionThrift;
import py.thrift.share.DriverIsUpgradingExceptionThrift;
import py.thrift.share.DriverKeyThrift;
import py.thrift.share.DriverLaunchingExceptionThrift;
import py.thrift.share.DriverMetadataThrift;
import py.thrift.share.DriverNameExistsExceptionThrift;
import py.thrift.share.DriverNotFoundExceptionThrift;
import py.thrift.share.DriverStatusThrift;
import py.thrift.share.DriverStillReportExceptionThrift;
import py.thrift.share.DriverTypeConflictExceptionThrift;
import py.thrift.share.DriverTypeIsConflictExceptionThrift;
import py.thrift.share.DriverTypeThrift;
import py.thrift.share.DriverUnmountingExceptionThrift;
import py.thrift.share.DynamicIoLimitationTimeInterleavingExceptionThrift;
import py.thrift.share.EndPointNotFoundExceptionThrift;
import py.thrift.share.EquilibriumVolumeRequest;
import py.thrift.share.EquilibriumVolumeResponse;
import py.thrift.share.ExistsClientExceptionThrift;
import py.thrift.share.ExistsDriverExceptionThrift;
import py.thrift.share.FailToRemoveArchiveFromStoragePoolExceptionThrift;
import py.thrift.share.FailToRemoveDatanodeFromDomainExceptionThrift;
import py.thrift.share.FailedToUmountDriverExceptionThrift;
import py.thrift.share.FixVolumeRequestThrift;
import py.thrift.share.FixVolumeResponseThrift;
import py.thrift.share.FreeArchiveStoragePoolRequestThrift;
import py.thrift.share.FreeDatanodeDomainRequestThrift;
import py.thrift.share.FrequentFixVolumeRequestThrift;
import py.thrift.share.GetAppliedIscsisRequest;
import py.thrift.share.GetAppliedIscsisResponse;
import py.thrift.share.GetAppliedRebalanceRulePoolRequest;
import py.thrift.share.GetAppliedRebalanceRulePoolResponse;
import py.thrift.share.GetAppliedStoragePoolsRequest;
import py.thrift.share.GetAppliedStoragePoolsResponse;
import py.thrift.share.GetAppliedVolumesRequest;
import py.thrift.share.GetAppliedVolumesResponse;
import py.thrift.share.GetCapacityRecordRequestThrift;
import py.thrift.share.GetCapacityRecordResponseThrift;
import py.thrift.share.GetCapacityRequest;
import py.thrift.share.GetCapacityResponse;
import py.thrift.share.GetConnectClientInfoRequest;
import py.thrift.share.GetConnectClientInfoResponse;
import py.thrift.share.GetDiskSmartInfoRequestThrift;
import py.thrift.share.GetDiskSmartInfoResponseThrift;
import py.thrift.share.GetDriverConnectPermissionRequestThrift;
import py.thrift.share.GetDriverConnectPermissionResponseThrift;
import py.thrift.share.GetIoLimitationAppliedDriversRequest;
import py.thrift.share.GetIoLimitationAppliedDriversResponse;
import py.thrift.share.GetIoLimitationRequestThrift;
import py.thrift.share.GetIoLimitationResponseThrift;
import py.thrift.share.GetIoLimitationsRequest;
import py.thrift.share.GetIoLimitationsResponse;
import py.thrift.share.GetIscsiAccessRulesRequest;
import py.thrift.share.GetIscsiAccessRulesResponse;
import py.thrift.share.GetMigrationRulesRequest;
import py.thrift.share.GetMigrationRulesResponse;
import py.thrift.share.GetPerformanceFromPyMetricsResponseThrift;
import py.thrift.share.GetPerformanceParameterRequestThrift;
import py.thrift.share.GetPerformanceParameterResponseThrift;
import py.thrift.share.GetPerformanceResponseThrift;
import py.thrift.share.GetPydDriverStatusExceptionThrift;
import py.thrift.share.GetRebalanceRuleRequest;
import py.thrift.share.GetRebalanceRuleResponse;
import py.thrift.share.GetScsiClientExceptionThrift;
import py.thrift.share.GetScsiDeviceOperationExceptionThrift;
import py.thrift.share.GetStoragePerformanceFromPyMetricsResponseThrift;
import py.thrift.share.GetStoragePerformanceParameterRequestThrift;
import py.thrift.share.GetUnAppliedRebalanceRulePoolRequest;
import py.thrift.share.GetUnAppliedRebalanceRulePoolResponse;
import py.thrift.share.GetVolumeAccessRulesRequest;
import py.thrift.share.GetVolumeAccessRulesResponse;
import py.thrift.share.GroupThrift;
import py.thrift.share.HardDiskInfoThrift;
import py.thrift.share.InfocenterServerExceptionThrift;
import py.thrift.share.InstanceIdAndEndPointThrift;
import py.thrift.share.InstanceIncludeVolumeInfoRequest;
import py.thrift.share.InstanceIncludeVolumeInfoResponse;
import py.thrift.share.InstanceIsSubHealthExceptionThrift;
import py.thrift.share.InstanceMetadataThrift;
import py.thrift.share.InstanceNotExistsExceptionThrift;
import py.thrift.share.InternalErrorThrift;
import py.thrift.share.InvalidGroupExceptionThrift;
import py.thrift.share.InvalidInputExceptionThrift;
import py.thrift.share.IoLimitationIsDeleting;
import py.thrift.share.IoLimitationThrift;
import py.thrift.share.IoLimitationTimeInterLeavingThrift;
import py.thrift.share.IoLimitationsDuplicateThrift;
import py.thrift.share.IoLimitationsNotExists;
import py.thrift.share.IscsiAccessRuleDuplicateThrift;
import py.thrift.share.IscsiAccessRuleFormatErrorThrift;
import py.thrift.share.IscsiAccessRuleNotFoundThrift;
import py.thrift.share.IscsiAccessRuleThrift;
import py.thrift.share.IscsiAccessRuleUnderOperationThrift;
import py.thrift.share.IscsiBeingDeletedExceptionThrift;
import py.thrift.share.IscsiNotFoundExceptionThrift;
import py.thrift.share.LackDatanodeExceptionThrift;
import py.thrift.share.LaunchDriverRequestThrift;
import py.thrift.share.LaunchDriverResponseThrift;
import py.thrift.share.LaunchScsiDriverRequestThrift;
import py.thrift.share.LaunchedVolumeCannotBeDeletedExceptionThrift;
import py.thrift.share.LimitTypeThrift;
import py.thrift.share.ListAccountsRequest;
import py.thrift.share.ListAccountsResponse;
import py.thrift.share.ListDomainRequest;
import py.thrift.share.ListDomainResponse;
import py.thrift.share.ListIoLimitationsRequest;
import py.thrift.share.ListIoLimitationsResponse;
import py.thrift.share.ListIscsiAccessRulesByDriverKeysRequest;
import py.thrift.share.ListIscsiAccessRulesByDriverKeysResponse;
import py.thrift.share.ListIscsiAccessRulesRequest;
import py.thrift.share.ListIscsiAccessRulesResponse;
import py.thrift.share.ListIscsiAppliedAccessRulesRequestThrift;
import py.thrift.share.ListIscsiAppliedAccessRulesResponseThrift;
import py.thrift.share.ListMigrationRulesRequest;
import py.thrift.share.ListMigrationRulesResponse;
import py.thrift.share.ListOperationsRequest;
import py.thrift.share.ListOperationsResponse;
import py.thrift.share.ListResourcesRequest;
import py.thrift.share.ListResourcesResponse;
import py.thrift.share.ListServerNodeByIdRequestThrift;
import py.thrift.share.ListServerNodeByIdResponseThrift;
import py.thrift.share.ListServerNodesRequestThrift;
import py.thrift.share.ListServerNodesResponseThrift;
import py.thrift.share.ListStoragePoolCapacityRequestThrift;
import py.thrift.share.ListStoragePoolCapacityResponseThrift;
import py.thrift.share.ListStoragePoolRequestThrift;
import py.thrift.share.ListStoragePoolResponseThrift;
import py.thrift.share.ListVolumeAccessRulesByVolumeIdsRequest;
import py.thrift.share.ListVolumeAccessRulesByVolumeIdsResponse;
import py.thrift.share.ListVolumeAccessRulesRequest;
import py.thrift.share.ListVolumeAccessRulesResponse;
import py.thrift.share.LoadVolumeExceptionThrift;
import py.thrift.share.MigrationRuleIsDeleting;
import py.thrift.share.MigrationRuleNotExists;
import py.thrift.share.MigrationRuleThrift;
import py.thrift.share.MountScsiDeviceRequest;
import py.thrift.share.MountScsiDeviceResponse;
import py.thrift.share.NetworkErrorExceptionThrift;
import py.thrift.share.NextActionInfoThrift;
import py.thrift.share.NextActionThrift;
import py.thrift.share.NoDriverLaunchExceptionThrift;
import py.thrift.share.NoEnoughPydDeviceExceptionThrift;
import py.thrift.share.NoMemberExceptionThrift;
import py.thrift.share.NotEnoughGroupExceptionThrift;
import py.thrift.share.NotEnoughSpaceExceptionThrift;
import py.thrift.share.NotRootVolumeExceptionThrift;
import py.thrift.share.OfflineDiskRequest;
import py.thrift.share.OfflineDiskResponse;
import py.thrift.share.OlderPasswordIncorrectExceptionThrift;
import py.thrift.share.OneDomainDisplayThrift;
import py.thrift.share.OneStoragePoolDisplayThrift;
import py.thrift.share.OnlineDiskRequest;
import py.thrift.share.OnlineDiskResponse;
import py.thrift.share.OperationNotFoundExceptionThrift;
import py.thrift.share.OperationThrift;
import py.thrift.share.PageMigrationSpeedInfoThrift;
import py.thrift.share.ParametersIsErrorExceptionThrift;
import py.thrift.share.PermissionNotGrantExceptionThrift;
import py.thrift.share.PoolAlreadyAppliedRebalanceRuleExceptionThrift;
import py.thrift.share.PrimaryMigrateThrift;
import py.thrift.share.ReadPerformanceParameterFromFileExceptionThrift;
import py.thrift.share.ReadWriteTypeThrift;
import py.thrift.share.RebalanceRuleExistingExceptionThrift;
import py.thrift.share.RebalanceRuleNotExistExceptionThrift;
import py.thrift.share.RebalanceRulethrift;
import py.thrift.share.RebalanceTaskListThrift;
import py.thrift.share.RebalanceTaskThrift;
import py.thrift.share.RemoveArchiveFromStoragePoolRequestThrift;
import py.thrift.share.RemoveArchiveFromStoragePoolResponseThrift;
import py.thrift.share.RemoveDatanodeFromDomainRequest;
import py.thrift.share.RemoveDatanodeFromDomainResponse;
import py.thrift.share.ReportDbResponseThrift;
import py.thrift.share.ReportIscsiAccessRulesRequest;
import py.thrift.share.ReportIscsiAccessRulesResponse;
import py.thrift.share.ReportServerNodeInfoRequestThrift;
import py.thrift.share.ReportServerNodeInfoResponseThrift;
import py.thrift.share.ResetAccountPasswordRequest;
import py.thrift.share.ResetAccountPasswordResponse;
import py.thrift.share.ResourceNotExistsExceptionThrift;
import py.thrift.share.ResourceThrift;
import py.thrift.share.RoleIsAssignedToAccountsExceptionThrift;
import py.thrift.share.RoleNotExistedExceptionThrift;
import py.thrift.share.RoleThrift;
import py.thrift.share.RootVolumeBeingDeletedExceptionThrift;
import py.thrift.share.RootVolumeNotFoundExceptionThrift;
import py.thrift.share.ScsiClientIsExistExceptionThrift;
import py.thrift.share.ScsiClientIsNotOkExceptionThrift;
import py.thrift.share.ScsiClientOperationExceptionThrift;
import py.thrift.share.ScsiDeviceIsLaunchExceptionThrift;
import py.thrift.share.ScsiDeviceStatusThrift;
import py.thrift.share.ScsiVolumeLockExceptionThrift;
import py.thrift.share.SecondaryMigrateThrift;
import py.thrift.share.SegIdThrift;
import py.thrift.share.SegmentExistingExceptionThrift;
import py.thrift.share.SegmentMembershipThrift;
import py.thrift.share.SegmentUnitBeingDeletedExceptionThrift;
import py.thrift.share.SegmentUnitMetadataThrift;
import py.thrift.share.SegmentUnitRoleThrift;
import py.thrift.share.SegmentUnitStatusConflictCauseThrift;
import py.thrift.share.SegmentUnitStatusThrift;
import py.thrift.share.SegmentUnitTypeThrift;
import py.thrift.share.SensorInfoThrift;
import py.thrift.share.ServerNodeIsUnknownThrift;
import py.thrift.share.ServerNodeNotExistExceptionThrift;
import py.thrift.share.ServerNodePositionIsRepeatExceptionThrift;
import py.thrift.share.ServerNodeThrift;
import py.thrift.share.ServiceHavingBeenShutdownThrift;
import py.thrift.share.ServiceIpStatusThrift;
import py.thrift.share.ServiceIsNotAvailableThrift;
import py.thrift.share.SetArchiveStoragePoolRequestThrift;
import py.thrift.share.SetDatanodeDomainRequestThrift;
import py.thrift.share.SetIscsiChapControlRequestThrift;
import py.thrift.share.SetIscsiChapControlResponseThrift;
import py.thrift.share.SettleArchiveTypeRequest;
import py.thrift.share.SettleArchiveTypeResponse;
import py.thrift.share.SnapshotRollingBackExceptionThrift;
import py.thrift.share.StillHaveStoragePoolExceptionThrift;
import py.thrift.share.StillHaveVolumeExceptionThrift;
import py.thrift.share.StorageEmptyExceptionThrift;
import py.thrift.share.StoragePoolCapacityThrift;
import py.thrift.share.StoragePoolExistedExceptionThrift;
import py.thrift.share.StoragePoolIsDeletingExceptionThrift;
import py.thrift.share.StoragePoolNameExistedExceptionThrift;
import py.thrift.share.StoragePoolNotExistInDoaminExceptionThrift;
import py.thrift.share.StoragePoolNotExistedExceptionThrift;
import py.thrift.share.StoragePoolThrift;
import py.thrift.share.SystemCpuIsNotEnoughThrift;
import py.thrift.share.SystemMemoryIsNotEnoughThrift;
import py.thrift.share.TooManyDriversExceptionThrift;
import py.thrift.share.TooManyEndPointFoundExceptionThrift;
import py.thrift.share.TransportExceptionThrift;
import py.thrift.share.TurnOffAllDiskLightByServerIdRequestThrift;
import py.thrift.share.TurnOffAllDiskLightByServerIdResponseThrift;
import py.thrift.share.UmountDriverRequestThrift;
import py.thrift.share.UmountDriverResponseThrift;
import py.thrift.share.UmountScsiDeviceRequest;
import py.thrift.share.UmountScsiDeviceResponse;
import py.thrift.share.UmountScsiDriverRequestThrift;
import py.thrift.share.UmountScsiDriverResponseThrift;
import py.thrift.share.UnApplyRebalanceRuleRequest;
import py.thrift.share.UnsupportedEncodingExceptionThrift;
import py.thrift.share.UpdateDiskLightStatusByIdRequestThrift;
import py.thrift.share.UpdateDiskLightStatusByIdResponseThrift;
import py.thrift.share.UpdateDomainRequest;
import py.thrift.share.UpdateDomainResponse;
import py.thrift.share.UpdateIoLimitationRulesRequest;
import py.thrift.share.UpdateIoLimitationsResponse;
import py.thrift.share.UpdatePasswordRequest;
import py.thrift.share.UpdatePasswordResponse;
import py.thrift.share.UpdateRebalanceRuleRequest;
import py.thrift.share.UpdateServerNodeRequestThrift;
import py.thrift.share.UpdateServerNodeResponseThrift;
import py.thrift.share.UpdateStoragePoolRequestThrift;
import py.thrift.share.UpdateStoragePoolResponseThrift;
import py.thrift.share.VolumeAccessRuleDuplicateThrift;
import py.thrift.share.VolumeAccessRuleThrift;
import py.thrift.share.VolumeBeingDeletedExceptionThrift;
import py.thrift.share.VolumeCanNotLaunchMultiDriversThisTimeExceptionThrift;
import py.thrift.share.VolumeCannotBeRecycledExceptionThrift;
import py.thrift.share.VolumeCyclingExceptionThrift;
import py.thrift.share.VolumeDeletingExceptionThrift;
import py.thrift.share.VolumeExistingExceptionThrift;
import py.thrift.share.VolumeFixingOperationExceptionThrift;
import py.thrift.share.VolumeHasNotBeenLaunchedExceptionThrift;
import py.thrift.share.VolumeInActionThrift;
import py.thrift.share.VolumeInExtendingExceptionThrift;
import py.thrift.share.VolumeInMoveOnlineDoNotHaveOperationExceptionThrift;
import py.thrift.share.VolumeIsAppliedWriteAccessRuleExceptionThrift;
import py.thrift.share.VolumeIsBeginMovedExceptionThrift;
import py.thrift.share.VolumeIsCloningExceptionThrift;
import py.thrift.share.VolumeIsConnectedByWritePermissionClientExceptionThrift;
import py.thrift.share.VolumeIsCopingExceptionThrift;
import py.thrift.share.VolumeIsMarkWriteExceptionThrift;
import py.thrift.share.VolumeIsMovingExceptionThrift;
import py.thrift.share.VolumeLaunchMultiDriversExceptionThrift;
import py.thrift.share.VolumeMetadataThrift;
import py.thrift.share.VolumeNameExistedExceptionThrift;
import py.thrift.share.VolumeNotAvailableExceptionThrift;
import py.thrift.share.VolumeNotFoundExceptionThrift;
import py.thrift.share.VolumeOriginalSizeNotMatchExceptionThrift;
import py.thrift.share.VolumeRecycleInformationThrift;
import py.thrift.share.VolumeSizeIllegalExceptionThrift;
import py.thrift.share.VolumeSizeNotMultipleOfSegmentSizeThrift;
import py.thrift.share.VolumeStatusThrift;
import py.thrift.share.VolumeWasRollbackingExceptionThrift;
import py.thrift.share.listZookeeperServiceStatusRequest;
import py.thrift.share.listZookeeperServiceStatusResponse;
import py.thrift.systemdaemon.service.SystemDaemon;
import py.utils.ValidateParam;
import py.volume.ExceptionType;
import py.volume.OperationFunctionType;
import py.volume.VolumeInAction;
import py.volume.VolumeMetadata;
import py.volume.VolumeMetadata.VolumeSourceType;
import py.volume.VolumeStatus;
import py.volume.VolumeType;


@SuppressWarnings("ALL")
public class InformationCenterImpl extends AbstractConfigurationServer
    implements InformationCenter.Iface, ThriftProcessorFactory {

  public static final Integer allHaveTargetTypeDisk = 1;
  public static final Integer allDoNotHaveTargetTypeDisk = -1;
  public static final Integer someHaveTargetTypeDisk = 0;
  public static final int defaultNotClientConnect = -1;
  private static final Logger logger = LoggerFactory.getLogger(InformationCenterImpl.class);
  
  private static final int amountOfRedundantDriverContainerCandidates = 2;
  private static final long mbSize = 1024L * 1024L;
  private static final long gbSize = mbSize * 1024L;
  private final InformationCenter.Processor<InformationCenter.Iface> processor;
  private final long poolCanNotRebuildForReserveSegUnit = 0L;
  private InformationCenterAppEngine informationCenterAppEngine;
  private VolumeStore volumeStore; // store the all volume;
  private VolumeStatusTransitionStore volumeStatusStore; // store all volumes need to process
  // their status;
  private SegmentUnitTimeoutStore segmentUnitTimeoutStore; // store all the segment unit to check
  // if their report time is
  // timeout;
  private InfoCenterAppContext appContext;
  private InstanceStore instanceStore;
  private DriverStore driverStore;
  private StorageStore storageStore;
  private VolumeRuleRelationshipStore volumeRuleRelationshipStore;
  private AccessRuleStore accessRuleStore;
  private IscsiRuleRelationshipStore iscsiRuleRelationshipStore;
  private IscsiAccessRuleStore iscsiAccessRuleStore;
  private IoLimitationStore ioLimitationStore;
  private MigrationRuleStore migrationRuleStore;
  private RebalanceRuleStore rebalanceRuleStore;
  private ServerNodeStore serverNodeStore;
  private DiskInfoStore diskInfoStore;
  private DomainStore domainStore;
  private StoragePoolStore storagePoolStore;
  private AccountStore accountStore;
  private ApiStore apiStore;
  private RoleStore roleStore;
  private ResourceStore resourceStore;
  private DriverContainerSelectionStrategy driverContainerSelectionStrategy;
  private long segmentSize;
  private long pageSize;
  private long deadVolumeToRemove;
  private int groupCount;
  private long nextActionTimeIntervalMs;
  private SelectionStrategy selectionStrategy;
  private NetworkConfiguration networkConfiguration;
  private CoordinatorClientFactory coordinatorClientFactory;
  private OrphanVolumeStore orphanVolumes;
  private CapacityRecordStore capacityRecordStore;
  private Map<String, Long> serverNodeReportTimeMap;
  // if this is a limited Capacity, the total size of volumes created should
  // should not exceed the capacity, otherwise
  // throw no enough space exception, default size: 1T
  private long userMaxCapacityBytes = Long.MAX_VALUE;
  // shutdown flag
  private boolean shutDownFlag = false;
  private long lastRefreshTime;
  private long actualFreeSpace;
  private int pageWrappCount;
  private int segmentWrappCount;
  private InstanceMaintenanceDbStore instanceMaintenanceDbStore;
  // shutdown Thread mux
  private AtomicBoolean shutdownThreadRunningFlag = new AtomicBoolean(false);
  private BackupDbManager backupDbManager;
  private SegmentUnitsDistributionManager segmentUnitsDistributionManager;
  private GetRebalanceTaskSweeper getRebalanceTaskSweeper;
  private int waitCollectVolumeInfoSecond;
  private InfoCenterConfiguration infoCenterConfiguration;

  
  private DriverContainerClientFactory driverContainerClientFactory;
  private GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory;

  private VolumeJobStoreDb volumeJobStoreDb;
  private RecoverDbSentryStore recoverDbSentryStore;
  private OperationStore operationStore;
  private InstanceMaintenanceDbStore instanceMaintenanceStore;


  private int timeout;

  private CreateSegmentUnitWorkThread createSegmentUnitWorkThread;

  private RebalanceTaskExecutor rebalanceTaskExecutor;

  private PySecurityManager securityManager;
  private RefreshTimeAndFreeSpace refreshTimeAndFreeSpace;
  private VolumeInformationManger volumeInformationManger;
  private CreateVolumeManager createVolumeManager;
  private InstanceVolumeInEquilibriumManger instanceVolumeInEquilibriumManger;
  private InstanceIncludeVolumeInfoManger instanceIncludeVolumeInfoManger;
  private ExceptionForOperation exceptionForOperation;

  private LockForSaveVolumeInfo lockForSaveVolumeInfo;
  private ReportVolumeManager reportVolumeManager;
  private Map<Long, Integer> volumeIdToSegmentWrapperCountMap;
  private ScsiClientStore scsiClientStore;
  private TaskStore taskStore;
  private ScsiDriverStore scsiDriverStore;
  private VolumeDelayStore volumeDelayStore;
  private VolumeRecycleStore volumeRecycleStore;


  private VolumeRecycleManager volumeRecycleManager;
  private DriverClientManger driverClientManger;


  private AllDriverClearAccessRulesManager allDriverClearAccessRulesManager =
      new AllDriverClearAccessRulesManager();
  private ReadWriteLock snapshotLock = new ReentrantReadWriteLock();
  private ReentrantLock volumeLauchOrUnmountForCsiLock = new ReentrantLock();
  private ServerStatusCheck serverStatusCheck;
  private ReentrantLock lockForReport;


  public InformationCenterImpl() {
    processor = new InformationCenter.Processor<>(this);
    refreshTimeAndFreeSpace = RefreshTimeAndFreeSpace.getInstance();
    refreshTimeAndFreeSpace.setLastRefreshTime(0);
    serverNodeReportTimeMap = new ConcurrentHashMap<>();
    this.volumeIdToSegmentWrapperCountMap = new ConcurrentHashMap<>();
    this.lockForReport = new ReentrantLock();
  }

  /**
   * change this method to static for unit test purpose.
   */
  public static void splitDatanodesByGroupId(ReserveSegUnitResult reserveSegUnitResult,
      List<InstanceMetadataThrift> firstList, List<InstanceMetadataThrift> secondList) {

    // split datanodes by group id, because we want to create two segment units at one time
    // must make sure that datanodes belong to one group can not split to two list
    Multimap<Integer, InstanceMetadataThrift> mapGroupIdToDatanodes = HashMultimap
        .<Integer, InstanceMetadataThrift>create();
    for (InstanceMetadataThrift instanceMetadataThrift : reserveSegUnitResult.getInstances()) {
      mapGroupIdToDatanodes
          .put(instanceMetadataThrift.getGroup().getGroupId(), instanceMetadataThrift);
    }

    Multimap<Integer, Integer> sortMap = create();
    for (Integer groupId : mapGroupIdToDatanodes.keySet()) {
      int size = mapGroupIdToDatanodes.get(groupId).size();
      sortMap.put(size, groupId);
    }
    List<Integer> descList = new ArrayList<>(sortMap.keySet());
    Collections.sort(descList);
    Collections.reverse(descList);
    for (Integer size : descList) {
      Collection<Integer> groupIds = sortMap.get(size);
      for (Integer groupId : groupIds) {
        if (firstList.size() >= secondList.size()) {
          secondList.addAll(mapGroupIdToDatanodes.get(groupId));
        } else {
          firstList.addAll(mapGroupIdToDatanodes.get(groupId));
        }
      }

    }
  }

  public void setWaitCollectVolumeInfoSecond(int waitCollectVolumeInfoSecond) {
    this.waitCollectVolumeInfoSecond = waitCollectVolumeInfoSecond;
  }

  // for unit test
  public void setBackupDbManager(BackupDbManager backupDbManager) {
    this.backupDbManager = backupDbManager;
  }

  
  public void initBackupDbManager(long roundTimeInterval, int maxBackupCount) {
    this.backupDbManager = new BackupDbManagerImpl(roundTimeInterval, maxBackupCount,
        volumeRuleRelationshipStore,
        accessRuleStore, domainStore, storagePoolStore, capacityRecordStore,
        accountStore, apiStore, roleStore, resourceStore, instanceStore,
        iscsiRuleRelationshipStore, iscsiAccessRuleStore, ioLimitationStore, migrationRuleStore,
        rebalanceRuleStore, volumeDelayStore, volumeRecycleStore, volumeJobStoreDb,
        recoverDbSentryStore);
  }

  @Override
  public TProcessor getProcessor() {
    return processor;
  }

  @Override
  public void ping() throws TException {
    logger.debug("pinged by a remote instance :", new Exception());
  }

  /**********    report begin . *************/

  @Override
  public void shutdown() throws TException {
    this.shutDownFlag = true;

    if (!shutdownThreadRunningFlag.compareAndSet(false, true)) {
      return;
    }

    Exception closeStackTrace = new Exception("Capture service shutdown signal");
    logger.warn("shutting down info center service. stack trace of this: ", closeStackTrace);

    Thread shutdownForcely = new Thread(new Runnable() {

      @Override
      public void run() {
        if (informationCenterAppEngine != null) {
          informationCenterAppEngine.stop();
        }
        if (serverStatusCheck != null) {
          serverStatusCheck.stop();
        }
      }
    });
    Thread shutdownThread = new Thread(new Runnable() {

      @Override
      public void run() {
        logger.info("InfoCenter is shutdown");
        try {
          Thread.sleep(10000);
          System.exit(0);
        } catch (Exception e) {
          logger.error("caught an exception", e);
        }
      }
    }, "InfoCenterShutdownThread");
    shutdownThread.start();
    shutdownForcely.start();
  }

  @Override
  public listZookeeperServiceStatusResponse listZookeeperServiceStatus(
      listZookeeperServiceStatusRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      EndPointNotFoundExceptionThrift, NetworkErrorExceptionThrift, TException {
    checkInstanceStatus("listZookeeperServiceStatus");
    logger.warn("listZookeeperServiceStatus, request is: {}", request);

    listZookeeperServiceStatusResponse response = new listZookeeperServiceStatusResponse();
    response.setRequestId(request.getRequestId());

    Map<String, String> zookeeperServiceStatus = serverStatusCheck.getZookeeperServiceStatus();
    for (Map.Entry<String, String> entry : zookeeperServiceStatus.entrySet()) {
      String endPoint = entry.getKey();
      String[] split = endPoint.split(":");
      String host = split[0];

      ServiceIpStatusThrift thrift = new ServiceIpStatusThrift();
      thrift.setHostname(host);
      thrift.setStatus(entry.getValue());
      response.addToZookeeperStatusList(thrift);
    }

    logger.warn("listZookeeperServiceStatus, response is: {}", response);
    return response;
  }

  /**
   * master save the volumeinfo which report from the follower HA current instance must master.
   ***/
  @Override
  public ReportVolumeInfoResponse reportVolumeInfo(ReportVolumeInfoRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {
    //check the instance status
    checkInstanceStatus("reportVolumeInfo");

    //
    return reportVolumeManager.reportVolumeInfo(request);
  }

  @Override
  public RecoverDatabaseResponse recoverDatabase()
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {
    checkInstanceStatus("recoverDatabase");

    logger.warn("recoverDatabase request");

    boolean success = backupDbManager.recoverDatabase();
    RecoverDatabaseResponse response = new RecoverDatabaseResponse(success);
    logger.warn("recoverDatabase response: {}", response);
    return response;
  }

  
  public void generateDiskFreeSpaceRatioEventData(RawArchiveMetadata archive,
      InstanceMetadata datanode,
      long freeSpaceRatio) {

    Map<String, String> userDefineParams = new HashMap<>();
    userDefineParams.put(UserDefineName.DiskName.name(), archive.getDeviceName());
    userDefineParams.put(UserDefineName.DiskID.name(), String.valueOf(archive.getArchiveId()));
    userDefineParams
        .put(UserDefineName.DiskHost.name(), String.valueOf(datanode.getEndpoint().split(":")[0]));
    EventDataWorker eventDataWorker = new EventDataWorker(PyService.INFOCENTER, userDefineParams);

    Map<String, Long> counters = new HashMap<>();
    counters.put(CounterName.DISK_FREE_SPACE_RATIO.name(), freeSpaceRatio);

    eventDataWorker.work(OperationName.Disk_Space.name(), counters);
  }

  @Override
  public ReportArchivesResponse reportArchives(ReportArchivesRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      InvalidInputExceptionThrift,
      InvalidGroupExceptionThrift, TException {

    //check the instance status
    checkInstanceStatus("reportArchives");

    InstanceMetadata instance;
    NextActionInfoThrift datanodeNextAction = new NextActionInfoThrift();
    Map<Long, NextActionInfoThrift> archiveMapNextAction = new HashMap<>();
    ReportDbResponseThrift reportDbResponse = null;
    Map<Long, py.thrift.share.PageMigrationSpeedInfoThrift> archiveId2MigrationLimits =
        new HashMap<>();
    Map<Long, String> archiveId2MigrationStrategy = new HashMap<>();
    Map<Long, py.thrift.share.CheckSecondaryInactiveThresholdThrift> arcMap = new HashMap<>();
    RebalanceTaskListThrift rebalanceTaskListThrift = null;

    //when Equilibrium ok, the data node update report table ok
    Set<Long> volumeUpdateReportTableOk = request.getVolumeUpdateReportTableOk();
    instanceVolumeInEquilibriumManger.removeVolumeWhenDatanodeUpdateOk(volumeUpdateReportTableOk);
    /* back the volume status to datanode **/
    Set<Long> backAllstoragePoolTheReportArchivesIn = new HashSet<>();
    /* pool id,  volume id, volume status ***/
    Map<Long, Map<Long, VolumeStatusThrift>> eachStoragePoolVolumesStatus = new HashMap<>();

    try {
      InstanceMetadataThrift instanceMetadataThrift = request.getInstance();
      if (instanceMetadataThrift == null) {
        logger.error("report archive is empty, the request:{}", request);
        throw new InvalidInputExceptionThrift();
      }

      Validate.notNull(instanceMetadataThrift.getArchiveMetadata());
      logger.info("reportArchives request:{}", request);

      logger.warn("reportArchives request's endpoint: {}, archiveId :{}, devName: {}",
          request.getInstance().getEndpoint(),
          request.getInstance().archiveMetadata.stream().map(ArchiveMetadataThrift::getArchiveId)
              .collect(Collectors.toSet()),
          request.getInstance().archiveMetadata.stream().map(ArchiveMetadataThrift::getDevName)
              .collect(Collectors.toSet()));

      logger.warn(
          "reportArchives getAlreadyMigratedPage {}, getMaxMigrationSpeed :{}, getMigrationSpeed:"
              + " {},"
              + " getAlreadyMigratedPage: {}, getTotalPageToMigrate: {} failed :{}, get "
              + "usedSpace{}, getTotalSpace {}",
          request.getInstance().archiveMetadata.stream()
              .map(ArchiveMetadataThrift::getAlreadyMigratedPage)
              .collect(Collectors.toSet()),
          request.getInstance().archiveMetadata.stream()
              .map(ArchiveMetadataThrift::getMaxMigrationSpeed)
              .collect(Collectors.toSet()),
          request.getInstance().archiveMetadata.stream()
              .map(ArchiveMetadataThrift::getMigrationSpeed)
              .collect(Collectors.toSet()),
          request.getInstance().archiveMetadata.stream()
              .map(ArchiveMetadataThrift::getAlreadyMigratedPage)
              .collect(Collectors.toSet()),
          request.getInstance().archiveMetadata.stream()
              .map(ArchiveMetadataThrift::getTotalPageToMigrate)
              .collect(Collectors.toSet()), request.getInstance().archiveMetadata.stream()
              .map(ArchiveMetadataThrift::getMigrateFailedSegIdList).collect(Collectors.toSet()),
          request.getInstance().archiveMetadata.stream()
              .map(ArchiveMetadataThrift::getLogicalUsedSpace)
              .collect(Collectors.toSet()),
          request.getInstance().archiveMetadata.stream()
              .map(ArchiveMetadataThrift::getLogicalFreeSpace)
              .collect(Collectors.toSet()));

      instance = RequestResponseHelper.buildInstanceFrom(instanceMetadataThrift);
      instance.setLastUpdated(System.currentTimeMillis());

      InstanceId instanceId = instance.getInstanceId();

      // check datanode domain info
      boolean datanodeInDomainStore = false;
      boolean domainIdChanged = false;
      boolean domainTimePassedEnough = false;
      boolean domainIsDeleting = false;
      Long domainId = null;
      List<Domain> allDomains = domainStore.listAllDomains();
      // find the latest update domain which contains datanodeId
      Domain latestUpdateDomain = null;
      for (Domain domain : allDomains) {
        if (domain.getDataNodes().contains(instanceId.getId())) {
          if (latestUpdateDomain == null) {
            latestUpdateDomain = domain;
            continue;
          } else {
            latestUpdateDomain = (
                latestUpdateDomain.getLastUpdateTime().compareTo(domain.getLastUpdateTime()) >= 0)
                ? latestUpdateDomain
                : domain;
          }
        }
      }

      if (latestUpdateDomain != null) {
        datanodeInDomainStore = true;
        domainTimePassedEnough = latestUpdateDomain.timePassedLongEnough(nextActionTimeIntervalMs);
        domainIsDeleting = latestUpdateDomain.isDeleting();
        if (!domainIsDeleting) {
          domainId = latestUpdateDomain.getDomainId();
        }
        if (!latestUpdateDomain.getDomainId().equals(instance.getDomainId())) {
          domainIdChanged = true;
        }
      }

      if (instance.isFree()) {
        // domain store show this instance is in one domain
        if (datanodeInDomainStore) {
          instance.setDomainId(domainId);
          // should wait enough time then new allocate domain id to datanode
          if (!domainIsDeleting && domainTimePassedEnough) {
            datanodeNextAction.setNextAction(NextActionThrift.NEWALLOC);
            datanodeNextAction.setNewId(domainId);
          } else {
            datanodeNextAction.setNextAction(NextActionThrift.KEEP);
          }
        } else {
          datanodeNextAction.setNextAction(NextActionThrift.KEEP);
        }
       
      } else {
       
        if (datanodeInDomainStore) {
          instance.setDomainId(domainId);
          if (domainIdChanged) {
            // also wait enough time
            if (!domainIsDeleting && domainTimePassedEnough) {
              datanodeNextAction.setNextAction(NextActionThrift.CHANGE);
              datanodeNextAction.setNewId(domainId);
            } else {
              datanodeNextAction.setNextAction(NextActionThrift.KEEP);
            }
          } else {
            datanodeNextAction.setNextAction(NextActionThrift.KEEP);
          }
        } else {
          
          if (backupDbManager.passedRecoveryTime()) {
            datanodeNextAction.setNextAction(NextActionThrift.FREEMYSELF);
            logger.warn("instance {} is no longer belong to any domain, set domainId to NULL",
                instance.getInstanceId().getId());
            instance.setFree();
          }
        }
      }

      List<RawArchiveMetadata> archives = instance.getArchives();

      //save all diskInfo of current instance
      Map<String, DiskInfo> instanceDiskInfoMap = new HashMap<>();    //<diskSn, DiskInfo>
      if (serverNodeStore != null || archives.size() > 0) {
        //get current instance ip
        String instanceIp = instance.getEndpoint();
        if (instanceIp != null && instanceIp.contains(":")) {
          instanceIp = instanceIp.substring(0, instanceIp.indexOf(":")).trim();
        }

        //get serverNode by ip
        ServerNode serverNode = serverNodeStore.getServerNodeByIp(instanceIp);
        if (serverNode != null) {
          //save all diskInfo of the serverNode
          for (DiskInfo diskInfo : serverNode.getDiskInfoSet()) {
            instanceDiskInfoMap.put(diskInfo.getSn(), diskInfo);
          }
        } else {
          logger.warn("cannot find any serverNodes of the instance ip({}) ");
        }

        if (instanceDiskInfoMap.size() > 0) {
          logger.debug("find diskInfo of the serverNode:{}", instanceDiskInfoMap, serverNode);
        } else {
          logger.warn("cannot find any diskInfo of the instance:{}", instanceDiskInfoMap, instance);
        }
      }

     
      for (RawArchiveMetadata archive : archives) {
        Long archiveId = archive.getArchiveId();
        NextActionInfoThrift archiveNextAction = new NextActionInfoThrift();
        archiveMapNextAction.put(archiveId, archiveNextAction);
        PageMigrationSpeedInfoThrift pageMigrationSpeedInfoThrift =
            new PageMigrationSpeedInfoThrift();
        archiveId2MigrationLimits.put(archiveId, pageMigrationSpeedInfoThrift);

        //set archives slotNo
        if (instanceDiskInfoMap.containsKey(archive.getSerialNumber())) {
          String slotNo = instanceDiskInfoMap.get(archive.getSerialNumber()).getSlotNumber();
          archive.setSlotNo(slotNo);
          logger.debug("set archive:{} slotNo:{}", archive, slotNo);
        }

        boolean archiveInStoragePoolStore = false;
        boolean storagePoolIdChanged = false;
        boolean storagePoolTimePassedEnough = false;
        boolean storagePoolIsDeleting = false;
        Long storagePoolId = null;
        StoragePool latestStoragePool = null;
        List<StoragePool> allStoragePools = storagePoolStore.listAllStoragePools();
        for (StoragePool storagePool : allStoragePools) {
          if (storagePool.getArchivesInDataNode().containsEntry(instanceId.getId(), archiveId)) {
            if (latestStoragePool == null) {
              latestStoragePool = storagePool;
              continue;
            } else {
              latestStoragePool = (
                  latestStoragePool.getLastUpdateTime().compareTo(storagePool.getLastUpdateTime())
                      >= 0) ? latestStoragePool : storagePool;
            }
          }
        }
        if (latestStoragePool != null) {
          archiveInStoragePoolStore = true;
          storagePoolTimePassedEnough = latestStoragePool
              .timePassedLongEnough(nextActionTimeIntervalMs);
          storagePoolIsDeleting = latestStoragePool.isDeleting();
          storagePoolId = latestStoragePool.getPoolId();

          CheckSecondaryInactiveThresholdThrift checkSecondaryInactiveThresholdThrift =
              new CheckSecondaryInactiveThresholdThrift();
          arcMap
              .put(archiveId, checkSecondaryInactiveThresholdThrift);

          MigrationRuleInformation migrationRuleInformation = migrationRuleStore
              .get(latestStoragePool.getMigrationRuleId());

          if (migrationRuleInformation != null) {
           
            pageMigrationSpeedInfoThrift
                .setMaxMigrationSpeed((migrationRuleInformation.getMaxMigrationSpeed()));
            archiveId2MigrationStrategy
                .put(archiveId, migrationRuleInformation.getMigrationStrategy());

            checkSecondaryInactiveThresholdThrift.setMode(CheckSecondaryInactiveThresholdModeThrift
                .valueOf(migrationRuleInformation.getCheckSecondaryInactiveThresholdMode()));
            checkSecondaryInactiveThresholdThrift
                .setStartTime(migrationRuleInformation.getStartTime());
            checkSecondaryInactiveThresholdThrift.setEndTime(migrationRuleInformation.getEndTime());
            checkSecondaryInactiveThresholdThrift
                .setWaitTime(migrationRuleInformation.getWaitTime());
            checkSecondaryInactiveThresholdThrift
                .setIgnoreMissPagesAndLogs(migrationRuleInformation.getIgnoreMissPagesAndLogs());

          } else {
           
            pageMigrationSpeedInfoThrift.setMaxMigrationSpeed(0);
            archiveId2MigrationStrategy.put(archiveId, MigrationStrategy.Smart.name());

            checkSecondaryInactiveThresholdThrift
                .setMode(CheckSecondaryInactiveThresholdModeThrift.RelativeTime);
            checkSecondaryInactiveThresholdThrift.setStartTime(-1);
            checkSecondaryInactiveThresholdThrift.setEndTime(-1);
            checkSecondaryInactiveThresholdThrift.setWaitTime(-1);
            checkSecondaryInactiveThresholdThrift.setIgnoreMissPagesAndLogs(false);
          }

          if (!latestStoragePool.getPoolId().equals(archive.getStoragePoolId())) {
            storagePoolIdChanged = true;
          }
        } else {
         
          pageMigrationSpeedInfoThrift.setMaxMigrationSpeed(0);
          archiveId2MigrationStrategy.put(archiveId, MigrationStrategy.Smart.name());
        }
       
        if (archive.isFree()) {
          if (archiveInStoragePoolStore) {
            archive.setStoragePoolId(storagePoolId);
            if (!storagePoolIsDeleting && storagePoolTimePassedEnough) {
              archiveNextAction.setNextAction(NextActionThrift.NEWALLOC);
              archiveNextAction.setNewId(storagePoolId);
            } else {
              archiveNextAction.setNextAction(NextActionThrift.KEEP);
            }
          } else {
            archiveNextAction.setNextAction(NextActionThrift.KEEP);
            archive.setStoragePoolId(PoolInfo.AVAILABLE_POOLID);
          }
        } else {
          if (archiveInStoragePoolStore) {
            archive.setStoragePoolId(storagePoolId);
            if (storagePoolIdChanged) {
              if (!storagePoolIsDeleting && storagePoolTimePassedEnough) {
                archiveNextAction.setNextAction(NextActionThrift.CHANGE);
                archiveNextAction.setNewId(storagePoolId);
              } else {
                archiveNextAction.setNextAction(NextActionThrift.KEEP);
              }
            } else {
              archiveNextAction.setNextAction(NextActionThrift.KEEP);
            }
          } else {
            
            if (backupDbManager.passedRecoveryTime()) {
              archiveNextAction.setNextAction(NextActionThrift.FREEMYSELF);
              archive.setFree();
              logger.warn("archive {} is no longer belong to any pool, set archive pool to free",
                  archive);
            }
          }
        }

        /* back current archive in which storagePool ***/
        if (archive.getStoragePoolId() != null) {
          backAllstoragePoolTheReportArchivesIn.add(archive.getStoragePoolId());
        }
      }

      for (StoragePool storagePool : storagePoolStore.listAllStoragePools()) {
        Long totalFreeSpace = 0L;
        Long totalLogicSpace = 0L;
        logger.debug("get storage pool:{}", storagePool);
        if (storagePool.getArchivesInDataNode() == null || storagePool.getArchivesInDataNode()
            .isEmpty()) {
          continue;
        }

        for (Entry<Long, Long> entry : storagePool.getArchivesInDataNode().entries()) {
          Long datanodeId = entry.getKey();
          Long archiveId = entry.getValue();

          InstanceMetadata datanode = storageStore.get(datanodeId);
          if (datanode != null) {
            RawArchiveMetadata archive = datanode.getArchiveById(archiveId);
            if (archive != null) {

              totalFreeSpace += archive.getLogicalFreeSpace();
              totalLogicSpace += archive.getLogicalSpace();
              long archiveFreeSpaceRatio = 0;

              if (archive.getLogicalSpace() == 0) {
                logger
                    .warn("get archive:{} at datanodeId:{}, find the logicalSpace is zero", archive,
                        datanodeId);
              } else {
                archiveFreeSpaceRatio = archive.getLogicalFreeSpace() == 0
                    ? 0 : (archive.getLogicalFreeSpace() * 100) / archive.getLogicalSpace();
              }
              generateDiskFreeSpaceRatioEventData(archive, datanode, archiveFreeSpaceRatio);

              logger.debug("get archive:{} at datanodeId:{}", archive, datanodeId);
            } else {
              logger
                  .warn("can not find archive info by archive Id:{}, at datanode id:{}", archiveId,
                      datanodeId);
            }
          } else {
            logger.warn("can not find datanode info by datanode Id:{}", datanodeId);
          }
        }
        logger
            .debug("get totalLogicSpace:{}, totalFreeSpace:{}, for storagePool:{}", totalLogicSpace,
                totalFreeSpace, storagePool.getPoolId());
        if (totalLogicSpace != 0) {
          Double usedRatio = (((double) totalLogicSpace - totalFreeSpace) / totalLogicSpace);
          storagePool.setUsedRatio(usedRatio);
          logger.debug("storage pool: {} usedRatio is {}", storagePool.getPoolId(),
              storagePool.getUsedRatio());
        }
      }

      /*
       * when two datanodes report at the same time, should consider sync problem
       */
      synchronized (storageStore) {
        InstanceMetadata oldInstance = storageStore.get(instanceId.getId());

        
        if (instance.getGroup() == null) {
          /* new group is null, old group is not null, give the old group to datanode **/
          if (oldInstance != null) {
            logger.warn("we set old group id {} to new instance", oldInstance.getGroup());
            instance.setGroup(oldInstance.getGroup());
          } else {

            /* new group is null, old group is null, assign a new group to  datanode **/
            final Map<Group, Long> group2Capacity = new HashMap<>();
            for (int i = 0; i < groupCount; i++) {
              group2Capacity.put(new Group(i), 0L);
            }

            // count the capacity of each group
            for (InstanceMetadata instanceFromStore : storageStore.list()) {
              Group currentGroup = instanceFromStore.getGroup();
              long currentCapacity = group2Capacity.get(currentGroup);

              group2Capacity
                  .put(currentGroup, currentCapacity + ((instanceFromStore.getCapacity() == 0)
                      ? 1L : instanceFromStore.getCapacity()));
            }

            SelectionStrategy frugalSelectionStrategy = new ComparisonSelectionStrategy<Group>(
                new Comparator<Group>() {

                  @Override
                  public int compare(Group o1, Group o2) {
                    if (group2Capacity.get(o1) > group2Capacity.get(o2)) {
                      return 1;
                    } else if (group2Capacity.get(o1) < group2Capacity.get(o2)) {
                      return -1;
                    } else if (o1.getGroupId() > o2.getGroupId()) {
                      return 1;
                    } else if (o1.getGroupId() < o2.getGroupId()) {
                      return -1;
                    }

                    return 0;
                  }
                });
            Group selectedGroup = frugalSelectionStrategy.select(group2Capacity.keySet(), 1).get(0);
            logger.warn("we generate a group {} for new instance: {}", selectedGroup,
                instance.getInstanceId().getId());
            instance.setGroup(selectedGroup);
          }
        }
        logger.debug("Save instance reported from datanode {} to storage store", instance);
        storageStore.save(instance);
      }

      
      segmentUnitsDistributionManager.updateSimpleDatanodeInfo(instance);

      /*
       * process report database request
       */
      try {
        reportDbResponse = backupDbManager.process(request.getReportDbRequest());
        logger.info("get report DB response:{}", reportDbResponse);
      } catch (Exception e) {
        logger.error("failed to process report DB request:{}", request.getReportDbRequest(), e);
      }

      
     
      rebalanceTaskListThrift = getRebalanceTaskSweeper.pullTask(instance.getInstanceId().getId());

      /* get each pool volume status ***/
      for (Long storagePoolId : backAllstoragePoolTheReportArchivesIn) {
        StoragePool storagePool = storagePoolStore.getStoragePool(storagePoolId);

        if (storagePool == null) {
          logger.warn("can not get the storagePool {}", storagePoolId);
          continue;
        }

        Map<Long, VolumeStatusThrift> eachVolumeStatus = new HashMap<>();
        if (eachStoragePoolVolumesStatus.containsKey(storagePoolId)) {
          eachVolumeStatus = eachStoragePoolVolumesStatus.get(storagePoolId);
        }

        for (Long volumeId : storagePool.getVolumeIds()) {
          VolumeMetadata volumeMetadata = volumeStore.getVolume(volumeId);
          if (volumeMetadata == null) {
            logger.warn("can not found volume {}", volumeId);
            continue;
          }

          VolumeStatus volumeStatus = volumeMetadata.getVolumeStatus();
          eachVolumeStatus.put(volumeId, volumeStatus.getVolumeStatusThrift());
        }

        eachStoragePoolVolumesStatus.put(storagePoolId, eachVolumeStatus);
      }

    } catch (TException e) {
      logger.error("caught an exception with:{}", request.getRequestId(), e);
      throw e;
    } catch (Exception e) {
      logger.error("caught an exception with:{}", request.getRequestId(), e);
      throw new TException(e);
    }

    //for volume Equilib, volume move from one instance to other instance
    Map<Long, Map<Long, Long>> volumeReportToInstanceEquilibriumBuildWithVolumeId =
        instanceVolumeInEquilibriumManger
            .getVolumeReportToInstanceEquilibriumBuildWithVolumeId();

    AtomicLong version = instanceVolumeInEquilibriumManger.getUpdateReportToInstancesVersion();

    //notity the datanode update the report volume table
    Map<Long, Map<Long, Long>> updateTheDatanodeReportTable = instanceVolumeInEquilibriumManger
        .getUpdateTheDatanodeReportTable();

    //check Equilibrium ok or not
    boolean equilibriumStatus = instanceVolumeInEquilibriumManger.equilibriumOk();

    ReportArchivesResponse response;
    try {
      response = new ReportArchivesResponse();
      response.setRequestId(request.getRequestId());
      response.setGroup(new GroupThrift(instance.getGroup().getGroupId()));
      response.setDatanodeNextAction(datanodeNextAction);
      response.setArchiveIdMapNextAction(archiveMapNextAction);
      response.setReportDbResponse(reportDbResponse);
      response.setArchiveIdMapMigrationSpeed(archiveId2MigrationLimits);
      response.setArchiveIdMapMigrationStrategy(archiveId2MigrationStrategy);
      response.setArchiveIdMapCheckSecondaryInactiveThreshold(
          arcMap);
      response.setRebalanceTasks(rebalanceTaskListThrift);
      response.setEachStoragePoolVolumesStatus(eachStoragePoolVolumesStatus);

      //for Equilibrium volume
      /* set the datanod report the volume to which instance ***/
      response
          .setVolumeReportToInstancesSameTime(volumeReportToInstanceEquilibriumBuildWithVolumeId);
      /* if have volume Equilibrium ok, the version ++, so the datanode change the report
       * table ***/
      response.setUpdateReportToInstancesVersion(version.get());
      response.setUpdateTheDatanodeReportTable(updateTheDatanodeReportTable);
      /* init the Equilibrium table, clean the  Equilibrium table, because:
       * 1. the all volumes Equilibrium ok
       * 2. the volumes Equilibrium not ok, but it
       * ***/
      response.setEquilibriumOkAndClearValue(equilibriumStatus);

    } catch (Exception e) {
      logger.error("failed to build response", e);
      throw e;
    }

    //print log for Equilibrium volume
    if (!volumeReportToInstanceEquilibriumBuildWithVolumeId.isEmpty()) {
      logger.warn(
          "get the Equilibrium volume info, volumeReport:{}, version :{}, updateTable :{}, "
              + "equilibriumStatus :{}",
          volumeReportToInstanceEquilibriumBuildWithVolumeId, version.get(),
          updateTheDatanodeReportTable,
          equilibriumStatus);
    }

    logger.info("reportArchives response: {}", response);
    return response;
  }

  /**
   * update driver information.
   */
  @Override
  public ReportDriverMetadataResponse reportDriverMetadata(ReportDriverMetadataRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {

    //check the instance status
    checkInstanceStatus("reportDriverMetadata");
    logger.warn("reportDriverMetadata request: {}", request);

    try {
      Map<Long, String> volumeDescriptionMap = new HashMap<>();
      Set<DriverClientKey> driverClientKeysReport = new HashSet<>();
      List<DriverMetadata> driverMetadataReport = new ArrayList<>();

      synchronized (driverStore) {
        final long currentTime = System.currentTimeMillis();
        List<DriverMetadata> oldDriversInOneContainerNeedDelete = driverStore
            .getByDriverContainerId(request.getDrivercontainerId());

        for (DriverMetadataThrift driverMetadataThrift : request.getDrivers()) {
          DriverMetadata reportingDriverMetadata = RequestResponseHelper
              .buildDriverMetadataFrom(driverMetadataThrift);
          DriverMetadata driverInStore = driverStore
              .get(reportingDriverMetadata.getDriverContainerId(),
                  reportingDriverMetadata.getVolumeId(),
                  reportingDriverMetadata.getDriverType(), reportingDriverMetadata.getSnapshotId());
          if (driverInStore == null && reportingDriverMetadata.getDriverStatus()
              .equals(DriverStatus.REMOVING)) {
            logger.warn(
                "reporting driver is removing and has been already released, no need to store it "
                    + "back.");
            continue;
          }
          // a driver report from driver container, save it to store
          reportingDriverMetadata.setDriverStatus(reportingDriverMetadata.getDriverStatus()
              .turnToNextStatusOnEvent(DriverStateEvent.NEWREPORT));
          reportingDriverMetadata.setLastReportTime(currentTime);
          //just for dirver lost in db, set the report time
          reportingDriverMetadata.setCreateTime(currentTime);
          if (driverInStore != null) {
            reportingDriverMetadata.setDriverName(driverInStore.getDriverName());
            reportingDriverMetadata
                .setDynamicIoLimitationId(driverInStore.getDynamicIoLimitationId());
            reportingDriverMetadata
                .setStaticIoLimitationId(driverInStore.getStaticIoLimitationId());

            reportingDriverMetadata.setCreateTime(driverInStore.getCreateTime());
            reportingDriverMetadata.setMakeUnmountForCsi(driverInStore.isMakeUnmountForCsi());
            oldDriversInOneContainerNeedDelete.remove(driverInStore);
          }

          VolumeMetadata volumeMetadata = volumeStore
              .getVolume(reportingDriverMetadata.getVolumeId());
          /* some time the volumeMetadata is null **/
          String volumeDescription = null;
          if (volumeMetadata != null) {
            String volumeName = volumeMetadata.getName();
            volumeDescription = volumeMetadata.getVolumeDescription();
            volumeDescriptionMap.put(volumeMetadata.getVolumeId(), volumeDescription);
            reportingDriverMetadata.setVolumeName(volumeName);
          }

          driverStore.save(reportingDriverMetadata);

          //save
          driverMetadataReport.add(reportingDriverMetadata);
        }

        //the report no have this drivers, so remove the drivers in db
        for (DriverMetadata driverToDelete : oldDriversInOneContainerNeedDelete) {
          //just for csi check
          VolumeMetadata volumeMetadata = volumeStore.getVolume(driverToDelete.getVolumeId());
          /* some time the volumeMetadata is null **/
          driverStore.delete(driverToDelete.getDriverContainerId(), driverToDelete.getVolumeId(),
              driverToDelete.getDriverType(), driverToDelete.getSnapshotId());
        }
      }

      //check the reportingDriver client info
      for (DriverMetadata driverMetadata : driverMetadataReport) {
        String volumeDescription = volumeDescriptionMap.get(driverMetadata.getVolumeId());
        Set<DriverClientKey> driverClientKeysTmep = driverClientManger
            .checkReportDriverClientInfo(driverMetadata, volumeDescription);
        driverClientKeysReport.addAll(driverClientKeysTmep);
      }

      //check the not reportingDriver client info
      driverClientManger
          .checkNotReportDriverClientInfoByDriverContainer(request.getDrivercontainerId(),
              driverClientKeysReport);
    } catch (Exception e) {
      logger.error("caught an exception", e);
      throw e;
    }
    // a table of volume id to driver, which is used to help determine role of FS server and
    // assistant

    ReportDriverMetadataResponse response = new ReportDriverMetadataResponse();
    response.setRequestId(request.getRequestId());
    logger.warn("reportDriverMetadata response: {}", response);
    return response;
  }

  @Override
  public ReportScsiDriverMetadataResponse reportScsiDriverMetadata(
      ReportScsiDriverMetadataRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {
    if (shutDownFlag) {
      throw new ServiceHavingBeenShutdownThrift();
    }

    if (InstanceStatus.SUSPEND == appContext.getStatus()) {
      logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
      throw new ServiceIsNotAvailableThrift().setDetail("I am suspend");
    }

    logger.warn("reportScsiDriverMetadata request: {}", request);

    long drivercontainerId = request.getDrivercontainerId();
    List<ScsiDeviceInfoThrift> scsiList = request.getScsiList();
    if (scsiList == null) {
      scsiList = new ArrayList<>();
    }

    try {
      synchronized (scsiDriverStore) {
        final long currentTime = System.currentTimeMillis();
        List<ScsiDriverMetadata> oldDriversInOneContainerNeedDelete = scsiDriverStore
            .getByDriverContainerId(drivercontainerId);

        for (ScsiDeviceInfoThrift scsiDeviceInfoThrift : scsiList) {
          ScsiDriverMetadata reportingScsiDriverMetadata = RequestResponseHelper
              .buildScsiDriverMetadataFrom(scsiDeviceInfoThrift, drivercontainerId);
          ScsiDriverMetadata scsiDriverMetadata = scsiDriverStore
              .get(reportingScsiDriverMetadata.getDriverKeyForScsi());

          // a driver report from driver container, save it to store
          reportingScsiDriverMetadata.setLastReportTime(currentTime);
          logger.info("get the value is :{}", reportingScsiDriverMetadata);
          if (scsiDriverMetadata != null) {
            oldDriversInOneContainerNeedDelete.remove(scsiDriverMetadata);
          }

          scsiDriverStore.save(reportingScsiDriverMetadata);
        }

        for (ScsiDriverMetadata scsiDriverMetadata : oldDriversInOneContainerNeedDelete) {
          scsiDriverStore.delete(scsiDriverMetadata.getDriverKeyForScsi().getDrivercontainerId(),
              scsiDriverMetadata.getDriverKeyForScsi().getVolumeId(),
              scsiDriverMetadata.getDriverKeyForScsi().getSnapshotId());
        }
      }
    } catch (Exception e) {
      logger.error("caught an exception", e);
      throw e;
    }

    ReportScsiDriverMetadataResponse response = new ReportScsiDriverMetadataResponse();
    response.setRequestId(request.getRequestId());
    logger.warn("reportScsiDriverMetadata response: {}", response);
    return response;
  }

  /**
   * This interface is called by data node. When segment unit recover failed
   */
  @Override
  public ReportSegmentUnitRecycleFailResponse reportSegmentUnitRecycleFail(
      ReportSegmentUnitRecycleFailRequest request)
      throws InternalErrorThrift, ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      TException {
    //check the instance status
    checkInstanceStatus("reportSegmentUnitRecycleFail");

    logger.warn("reportSegmentUnitRecycleFail request: {}", request);
    // store the root volume which need to turn back again
    Set<Long> volumeNeedToDeletingAgain = new HashSet<>();

    for (SegmentUnitMetadataThrift segmentUnit : request.getSegUnitsMetadata()) {
      logger.warn("segment unit recover faild: segment unit {}", segmentUnit);
      VolumeMetadata volume = null;
      try {
        volume = volumeStore.getVolume(segmentUnit.getVolumeId());
      } catch (Exception e) {
        logger.warn("Can not get volume ", e);
      }

      if (volume != null) {
        volumeNeedToDeletingAgain.add(volume.getVolumeId());
      }
    }

    // set the volume deleting again
    for (Long volumeId : volumeNeedToDeletingAgain) {
      VolumeMetadata volume = null;
      try {
        volume = volumeStore.getVolume(volumeId);
      } catch (Exception e) {
        logger.warn("Can not get volume ", e);
      }

      // deleting all volume include children volume
      if (volume != null) {
        // set the volume status is deleting;
        volume.setVolumeStatus(VolumeStatus.Deleting);
        volumeStore
            .updateStatusAndVolumeInAction(volume.getVolumeId(), VolumeStatus.Deleting.toString(),
                DELETING.name());
      }
    }

    ReportSegmentUnitRecycleFailResponse response = new ReportSegmentUnitRecycleFailResponse(
        request.getRequestId());
    logger.warn("reportSegmentUnitRecycleFail response: {}", response);
    return response;
  }

  @Override
  public ReportSegmentUnitCloneFailResponse reportCloneFailed(
      ReportSegmentUnitCloneFailRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      VolumeNotFoundExceptionThrift,
      TException {
    //check the instance status
    checkInstanceStatus("reportCloneFailed");

    logger.warn("reportCloneFailed request: {}", request);
    long volumeId = request.getVolumeId();

    VolumeMetadata volumeMetadata = volumeStore.getVolume(volumeId);
    if (volumeMetadata == null) {
      logger.warn("when reportCloneFailed, but can not find the volume :{}", volumeId);
      throw new VolumeNotFoundExceptionThrift();
    }

    if (!volumeMetadata.isDeletedByUser()) {
      volumeMetadata.setVolumeStatus(VolumeStatus.Deleting);
      volumeStore.updateStatusAndVolumeInAction(volumeMetadata.getVolumeId(),
          VolumeStatus.Deleting.toString(),
          DELETING.name());
      logger.warn("when clone Failed, delete volume {}, volume status change to {}", volumeMetadata,
          VolumeStatus.Deleting);
    }

    ReportSegmentUnitCloneFailResponse response = new ReportSegmentUnitCloneFailResponse(
        request.getRequestId());
    logger.warn("reportCloneFailed response: {}", response);
    return response;
  }

  @Override
  public ReportJustCreatedSegmentUnitResponse reportJustCreatedSegmentUnit(
      ReportJustCreatedSegmentUnitRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {
    //check the instance status
    checkInstanceStatus("reportJustCreatedSegmentUnit");

    logger.warn("reportJustCreatedSegmentUnit request: {}", request);
    SegmentUnitMetadata segUnit = RequestResponseHelper
        .buildSegmentUnitMetadataFrom(request.getSegUnitMeta());
    CreateSegmentUnitsRequest segmentCreator = createVolumeManager
        .getSegmentCreator(segUnit.getSegId());
    if (null == segmentCreator) {
      ReportJustCreatedSegmentUnitResponse response = new ReportJustCreatedSegmentUnitResponse(
          request.getRequestId());
      logger.warn("reportJustCreatedSegmentUnit response: {}", response);
      return response;
    }

    SegmentUnitStatus myStatus = segUnit.getStatus();
    Validate.isTrue(SegmentUnitStatus.Primary == myStatus,
        "Invalid segment unit with invalid status" + myStatus);

    // A segment unit became primary right after being created, that means
    // the segment created done. Since that,
    // close the segment creator.
    segmentCreator.close();

    ReportJustCreatedSegmentUnitResponse response = new ReportJustCreatedSegmentUnitResponse(
        request.getRequestId());
    logger.warn("reportJustCreatedSegmentUnit response: {}", response);
    return response;
  }

  @Override
  public ReportSegmentUnitsMetadataResponse reportSegmentUnitsMetadata(
      ReportSegmentUnitsMetadataRequest request)
      throws ServiceIsNotAvailableThrift, ServiceHavingBeenShutdownThrift, TException {
    try {
      if (shutDownFlag) {
        throw new ServiceHavingBeenShutdownThrift();
      }

      if (InstanceStatus.SUSPEND != appContext.getStatus() && InstanceStatus.HEALTHY != appContext
          .getStatus()) {
        logger.error("current instance is not good, request content: {}", request);
        throw new ServiceIsNotAvailableThrift().setDetail("I am error");
      }

      logger.info("reportSegmentUnitsMetadata request :{}", request);
      logger.warn("reportSegmentUnits, from instance :{}, hava volume: {} to report,",
          request.getInstanceId(),
          request.getSegUnitsMetadata().stream().map(v -> v.getVolumeId())
              .collect(Collectors.toSet()));

      // this warn log is for integtest
      logger.info("report segmentUnitsMetadata count is: {}",
          request.getSegUnitsMetadata() == null ? 0 : request.getSegUnitsMetadata().size());

      /* respose ***/
      List<SegmentUnitMetadataThrift> returnedSegUnits = new ArrayList<>();
      InstanceId fromInstanceId = new InstanceId(request.getInstanceId());

      // this list is used to restore the conflict segment units
      List<SegUnitConflictThrift> conflictSegmentUnits = new ArrayList<>();

      //just for master which tell datanode to report volume to follower instance, not report to
      // master next time
      Map<Long, Set<Long>> whichhathisvolumetoreportmap = new HashMap<>();

      //tell datanode not report this voume to current instance
      Map<Long, Set<Long>> volumeNotToReportMap = new HashMap<>();
      Set<Long> volumesNotToReportToThisInstance = new HashSet<>();

      ReportSegmentUnitsMetadataResponse response = new ReportSegmentUnitsMetadataResponse(
          request.getRequestId(),
          conflictSegmentUnits, returnedSegUnits, whichhathisvolumetoreportmap,
          volumeNotToReportMap);

      /* do SegmentUnitMetadata  **/
      //get the master
      long currentInstanceId = appContext.getInstanceId().getId();
      for (SegmentUnitMetadataThrift receivedUnitMetadataThrift : request.getSegUnitsMetadata()) {
        long volumeId = receivedUnitMetadataThrift.getVolumeId();

        /*1. check the volume is in delete table, clenr the report volume,set it not report
         * next time ***/
        Set<Long> toDeleteVolumes = volumeInformationManger.getVolumeToDeleteForEquilibrium();
        if (toDeleteVolumes.contains(volumeId)) {
          //save
          volumesNotToReportToThisInstance.add(volumeId);
          volumeNotToReportMap.put(currentInstanceId, volumesNotToReportToThisInstance);
          logger.warn(
              "when reportSegmentUnitsMetadata, not report this volume:{} to current instance :{} ",
              volumeId, currentInstanceId);

          //set which HA to report
          continue;
        }

        SegmentUnitMetadata segUnit = RequestResponseHelper
            .buildSegmentUnitMetadataFrom(receivedUnitMetadataThrift);
        segUnit.setInstanceId(fromInstanceId);

        logger.info(
            "report segmentUnitsMetadata: segIndex:{}, instanceId:{}, memberShip:P:{}, S:{}, A:{}",
            segUnit.getSegId(), segUnit.getInstanceId(), segUnit.getMembership().getPrimary(),
            segUnit.getMembership().getSecondaries(), segUnit.getMembership().getArbiters());
        logger.info("received UnitMetadata is {}", segUnit);

        /* 2. first to get full volume in memory **/
        VolumeMetadata volumeMetadata;
        volumeMetadata = volumeStore.getVolumeForReport(volumeId);
        if (volumeMetadata != null) {
          volumeMetadata.setSegmentSize(segmentSize);
          logger.debug(
              "when reportSegmentUnitsMetadata, volume form report, volume id:{} {}, status:{}, "
                  + "action:{}",
              volumeId, volumeMetadata.getName(), volumeMetadata.getVolumeStatus(),
              volumeMetadata.getInAction());
        } else {

          /*  get for db  **/
          if (InstanceStatus.HEALTHY == appContext.getStatus()) {
            volumeMetadata = volumeStore.getVolume(volumeId);
            if (volumeMetadata != null) {
              logger.debug(
                  "when reportSegmentUnitsMetadata, the master get volume, volume id:{} {}, "
                      + "status:{}, action:{}",
                  volumeId, volumeMetadata.getName(), volumeMetadata.getVolumeStatus(),
                  volumeMetadata.getInAction());
            }
          } else {
            //folloer must get form db
            volumeMetadata = volumeStore.followerGetVolume(volumeId);
            if (volumeMetadata != null) {
              logger.debug(
                  "when reportSegmentUnitsMetadata, the follower get volume, volume id:{} {}, "
                      + "status:{}, action:{}",
                  volumeId, volumeMetadata.getName(), volumeMetadata.getVolumeStatus(),
                  volumeMetadata.getInAction());
            }
          }

          if (volumeMetadata != null) {
            volumeMetadata.setSegmentSize(segmentSize);

          } else {
            logger.error("reportSegmentUnitsMetadata, can not find volume :{}", volumeId);
          }
        }

        
        boolean volumeEquilibriumInToInstance = false;

       
        Pair<Boolean, Long> volumeFormNewInstance = instanceVolumeInEquilibriumManger
            .checkVolumeFormNewInstanceEquilibrium(volumeId, currentInstanceId);

        //volume in Equilibrium or not
        volumeEquilibriumInToInstance = volumeFormNewInstance.getFirst();
        if (volumeEquilibriumInToInstance) {
          logger.warn(
              "when reportSegmentUnitsMetadata, current volume :{} is report by equilibrium To "
                  + "instance,"
                  + "so not distribute it", volumeId);
        }

        /*4. check the current unit, which HA to save or report unit to which HA instance
         *  just the master can distribute Volume, if form Equilibrium to instance, not
         *  distribute it
         * ***/
        long getInstanceForReport = 0;
        if (InstanceStatus.HEALTHY == appContext.getStatus() && !volumeEquilibriumInToInstance) {
          logger.info("begin get the volume distribute value : {}",
              instanceIncludeVolumeInfoManger.printMapValue());

          //check in master or not
          boolean volumeInMaster = volumeInformationManger
              .checkCurrentVolumeInMaster(currentInstanceId, volumeId);
          if (!volumeInMaster) {
            //begin to distribute
            getInstanceForReport = volumeInformationManger.distributeVolumeToReportUnit(volumeId);
            if (getInstanceForReport == 0) {
              logger.warn("can not find the master instance, get next time");
              break;
            }

            /* save the volume in table, get all instance have the volume ***/
            instanceIncludeVolumeInfoManger.saveVolumeInfo(getInstanceForReport, volumeId);

            //current unit repot to other HA, so no need do it, save the volume id in table, for
            // master
            if (currentInstanceId != getInstanceForReport) {
              String volumeName = null;
              if (volumeMetadata != null) {
                volumeName = volumeMetadata.getName();
              }
              logger.warn("when choose,make the volume :{} {} report to follower instance :{}, "
                      + "and master id :{}, the distribute table :{}", volumeId, volumeName,
                  getInstanceForReport, currentInstanceId,
                  instanceIncludeVolumeInfoManger.printMapValue());

              /* save in table which report to datanode, set the next time report HA instance **/
              Set<Long> volumestoreportotherhas = new HashSet<>();
              if (whichhathisvolumetoreportmap.containsKey(getInstanceForReport)) {
                volumestoreportotherhas = whichhathisvolumetoreportmap.get(getInstanceForReport);
              }

              volumestoreportotherhas.add(volumeId);
              whichhathisvolumetoreportmap.put(getInstanceForReport, volumestoreportotherhas);

              //save
              volumesNotToReportToThisInstance.add(volumeId);
              volumeNotToReportMap.put(currentInstanceId, volumesNotToReportToThisInstance);
              logger.warn("for choose, not report this volume:{} to current instance :{} ",
                  volumeId, currentInstanceId);

              continue;
            }
          }
        }

        if (volumeMetadata != null) {
          try {
            volumeMetadata = processSegmentUnitWithVolumeExist(volumeMetadata, segUnit);
          } catch (Exception e) {
            logger.info("catch an exception", e);
            processExceptionWithSegmentUnitReport(conflictSegmentUnits, segUnit.getSegId(), e);
          }
        } else {
            //if the volume lost in db, just the master can rebuild volume
            if (InstanceStatus.OK == appContext.getStatus()) {
                volumeMetadata = processSegmentUnitWithVolumeNotExist(segUnit);
            } else {
                //save
                volumesNotToReportToThisInstance.add(volumeId);
                volumeNotToReportMap.put(currentInstanceId, volumesNotToReportToThisInstance);
                logger.warn("volume lost in db, not report this volume:{} to current instance :{} ",
                        volumeId, currentInstanceId);
            }
        }

        if (volumeMetadata == null) {
          logger.warn("Still can not find volume after process the segment unit with volume {}",
              segUnit.getSegId().getVolumeId().getId());
          returnedSegUnits.add(receivedUnitMetadataThrift);
        } else {
          // if in extend, and the current index in extend segment,
          if (receivedUnitMetadataThrift.getStatus().equals(SegmentUnitStatusThrift.Primary)) {
            if (checkVolumInExtendStyle(volumeMetadata, segUnit) == VolumeExtendStyle.InExtending) {
              updateSegmentsFreeRatioForExtend(receivedUnitMetadataThrift, volumeMetadata);
            }

            if (checkVolumInExtendStyle(volumeMetadata, segUnit) == VolumeExtendStyle.Normal) {
              updateSegmentsFreeRatio(receivedUnitMetadataThrift, volumeMetadata);
            }
          }
          volumeStore.saveVolumeForReport(volumeMetadata);
        }
      }

      if (InstanceStatus.HEALTHY == appContext.getStatus()) {
        logger.info("end get the volume distribute value : {}",
            instanceIncludeVolumeInfoManger.printMapValue());
      }

      if (!response.getWhichHaThisVolumeToReport().isEmpty() || !response
          .getVolumeNotToReportCurrentInstance()
          .isEmpty()) {
        logger.warn(
            "reportSegmentUnitsMetadata response, get the VolumeToReport: {} and NotToReport:{} ",
            response.getWhichHaThisVolumeToReport(),
            response.getVolumeNotToReportCurrentInstance());
      }

      if (!response.getConflicts().isEmpty()) {
        logger
            .warn("reportSegmentUnitsMetadata response, getConflicts: {}", response.getConflicts());
      }

      logger.info("reportSegmentUnitsMetadata response: {}", response);
      return response;
    } finally {
      logger.info("nothing need to do here");
    }
  }

  /**
   * add free space ratio to segment metadata & volume metadata.
   */
  private void updateSegmentsFreeRatio(SegmentUnitMetadataThrift tsegUnitsMetadata,
      VolumeMetadata volumeMetadata) {
    logger.debug("Going to get free space ratio of volume: {}", volumeMetadata.getVolumeId());
   
    if (tsegUnitsMetadata.getVolumeId() == volumeMetadata.getVolumeId()) {
      SegmentMetadata segMetadata = volumeMetadata
          .getSegmentByIndex(tsegUnitsMetadata.getSegIndex());
      double segmentFreeSpaceRatio = tsegUnitsMetadata.getRatioFreePages();
      logger.debug("Going to set segment ratio to {},current segment is {}", segmentFreeSpaceRatio,
          segMetadata.getIndex());
      segMetadata.setFreeRatio(segmentFreeSpaceRatio);
    }
  }

  /**
   * add free space ratio to segment metadata & volume metadata.
   */
  private void updateSegmentsFreeRatioForExtend(SegmentUnitMetadataThrift tsegUnitsMetadata,
      VolumeMetadata volumeMetadata) {
    logger.debug("Going to get free space ratio of volume: {}", volumeMetadata.getVolumeId());
    // modify the specified segments
    if (tsegUnitsMetadata.getVolumeId() == volumeMetadata.getVolumeId()) {
      SegmentMetadata segMetadata = volumeMetadata
          .getExtendSegmentByIndex(tsegUnitsMetadata.getSegIndex());
      double segmentFreeSpaceRatio = tsegUnitsMetadata.getRatioFreePages();
      logger.debug("Going to set segment ratio to {},current segment is {}", segmentFreeSpaceRatio,
          segMetadata.getIndex());
      segMetadata.setFreeRatio(segmentFreeSpaceRatio);
    }
  }

  private void processExceptionWithSegmentUnitReport(
      List<SegUnitConflictThrift> conflictSegmentUnits, SegId segId,
      Exception e) {
    if (e instanceof SegmentUnitStatusConflictExeption) {
      SegmentUnitStatusConflictExeption statusException = (SegmentUnitStatusConflictExeption) e;
      SegUnitConflictThrift conflict = new SegUnitConflictThrift(segId.getVolumeId().getId(),
          segId.getIndex(),
          SegmentUnitStatusConflictCauseThrift
              .valueOf(statusException.getConflictCause().toString()));
      conflictSegmentUnits.add(conflict);
    } else if (e instanceof VolumeExtendFailedException) {
      VolumeExtendFailedException statusException = (VolumeExtendFailedException) e;
      SegUnitConflictThrift conflict = new SegUnitConflictThrift(segId.getVolumeId().getId(),
          segId.getIndex(),
          SegmentUnitStatusConflictCauseThrift
              .valueOf(statusException.getConflictCause().toString()));
      conflictSegmentUnits.add(conflict);
    } else {
      logger.warn("an exception we can not handle {}", e.getClass().getSimpleName());
    }
  }

  
  private VolumeMetadata processSegmentUnitWithVolumeNotExist(SegmentUnitMetadata segUnit) {
    VolumeMetadata volume;

    // volume not exist and segment unit is deleted and deleting, return
    // directly;
    if (segUnit.getStatus() == SegmentUnitStatus.Deleted
        || segUnit.getStatus() == SegmentUnitStatus.Deleting) {
      return null;
    }

    // from last report time, it has been more than 6 months. Create a fake
    // volume
    long currentTimeSecond = Math.round(System.currentTimeMillis() / 1000f);
    if ((currentTimeSecond - deadVolumeToRemove) > Math.round(segUnit.getLastUpdated() / 1000f)) {

      if (segUnit.getSegId().getIndex() != 0) {
        volume = createFakeVolume(segUnit);
        updateSegmentUnitToVolume(volume, segUnit);
        return volume;
      }
    }

    // First segment, create an volume. Info center rebuild the data of
    // volume
    if (segUnit.getSegId().getIndex() == 0) {
      volume = processFirstSegmentInVolume(null, segUnit);
      if (volume == null) {
        logger.warn(
            "After process the first segment, the volume is still none. The segment unit is {}",
            segUnit);
      } else {
        logger.warn("new volume reported {}", volume);
        updateSegmentUnitToVolume(volume, segUnit);
      }
      return volume;
    }

    return null;
  }

  private VolumeExtendStyle checkVolumInExtendStyle(VolumeMetadata volume,
      SegmentUnitMetadata segUnit) {
    int currentIndex = segUnit.getSegId().getIndex();
    int lastIndex = volume.getSegmentCount();
    //for extend volume
    if ((currentIndex >= lastIndex) && volume.getExtendingSize() > 0) {
      logger.warn("checkVolumInExtendStyle for volume :{}, in index :{}, the unit for extend",
          volume.getVolumeId(), currentIndex);
      return VolumeExtendStyle.InExtending;
    }

    if ((currentIndex >= lastIndex) && volume.getExtendingSize() == 0) {
      return VolumeExtendStyle.ExtendFailed;
    }

    return VolumeExtendStyle.Normal;
  }

  private VolumeMetadata processSegmentUnitWithVolumeExist(VolumeMetadata volume,
      SegmentUnitMetadata segUnit)
      throws SegmentUnitStatusConflictExeption,
      VolumeExtendFailedException, IOException {
    // segment unit is the first segment, process first segment in volume
    int currentIndex = segUnit.getSegId().getIndex();
    if (currentIndex == 0) {
      volume = processFirstSegmentInVolume(volume, segUnit);
    }

    //for extend volume
    boolean extendVolumeFailed = false;
    if (checkVolumInExtendStyle(volume, segUnit) == VolumeExtendStyle.InExtending) {
      updateSegmentUnitToVolumeExtend(volume, segUnit);
    } else if (checkVolumInExtendStyle(volume, segUnit) == VolumeExtendStyle.Normal) {
      updateSegmentUnitToVolume(volume, segUnit);
    } else {
      extendVolumeFailed = true;
    }

   
   
   
   
   
   
   

    if (volume.isDeletedByUser()) {
      if (segUnit.getStatus() != SegmentUnitStatus.Deleted
          && segUnit.getStatus() != SegmentUnitStatus.Deleting
          && segUnit.getStatus() != SegmentUnitStatus.Primary) {
        throw new SegmentUnitStatusConflictExeption(SegmentUnitStatusConflictCause.VolumeDeleted);
      }
    }

   
   
    if (volume.isRecycling()) {
      if (segUnit.getStatus() == SegmentUnitStatus.Deleting) {
        throw new SegmentUnitStatusConflictExeption(SegmentUnitStatusConflictCause.VolumeRecycled);
      }
    }

    //last do the extend failed
    if (extendVolumeFailed) {
      throw new VolumeExtendFailedException(SegmentUnitStatusConflictCause.VolumeExtendFailed);
    }
    return volume;
  }

  private void updateSegmentUnitToVolume(VolumeMetadata volume, SegmentUnitMetadata newSegUnit) {
    if (newSegUnit.isSecondaryCandidate()) {
      logger
          .warn("when update, the segUnit :{} is candidate, not need save", newSegUnit.getSegId());
      return;
    }

    SegmentMetadata segmentInVolume = volume.getSegmentByIndex(newSegUnit.getSegId().getIndex());

    if (segmentInVolume == null) { // segment does not exist, create a new
      // one
      SegId segId = new SegId(newSegUnit.getSegId());
      SegmentMetadata newSegment = new SegmentMetadata(segId, newSegUnit.getSegId().getIndex());
      newSegment.putSegmentUnitMetadata(newSegUnit.getInstanceId(), newSegUnit);
      logger.info("Create a newSegment and add a new segment unit to volume: {}", newSegUnit);
      volume.addSegmentMetadata(newSegment, newSegUnit.getMembership());
     
     
      volumeStatusStore.addVolumeToStore(volume);
      // put it in the segment unit store to monitor if timeout
      segmentUnitTimeoutStore.addSegmentUnit(newSegUnit);
    } else {
      // compare current membership, if member ship reported from data
      // node is new, update membership
      SegmentMembership oldMembership = volume.getMembership(segmentInVolume.getIndex());
      SegmentMembership highestMembership = newSegUnit.getMembership();
      SegmentUnitMetadata oldSegUnit = segmentInVolume
          .getSegmentUnitMetadata(newSegUnit.getInstanceId());

      if (oldSegUnit == null) {
        // put the segment unit reported in store to monitor if timeout;
        segmentUnitTimeoutStore.addSegmentUnit(newSegUnit);
        logger.info("Add a new segment unit to volume: {}", newSegUnit);
        segmentInVolume.putSegmentUnitMetadata(newSegUnit.getInstanceId(), newSegUnit);
        if (segmentInVolume.isSegmentStatusChanged()) {
          
          logger.debug(
              "We put the volume in status store, for the segment status is changed. volume is "
                  + "{}, segment is {}",
              volume, segmentInVolume);
          volumeStatusStore.addVolumeToStore(volume);
        }
      } else {
        // update the content with report segment unit
        if (!oldSegUnit.equals(newSegUnit)) {
          
          oldSegUnit.updateWithNewOne(newSegUnit);
          logger.debug("After update a segment unit: {}", oldSegUnit);
          /* update member ship */
          if (oldMembership == null || oldMembership.compareTo(highestMembership) < 0) {
            volume.addSegmentMetadata(segmentInVolume, highestMembership);
          }

          
          if (segmentInVolume.isSegmentStatusChanged()) {
            logger.debug(
                "put the volume in status store, for the segment status is changed. volume is {},"
                    + " segment is {}",
                volume, segmentInVolume);
            volumeStatusStore.addVolumeToStore(volume);
          }
        }

        oldSegUnit.setMigrationStatus(newSegUnit.getMigrationStatus());
        oldSegUnit.setRatioMigration(newSegUnit.getRatioMigration());
        oldSegUnit.setLastReported(newSegUnit.getLastReported());
      }
    }
  }

  
  private void updateSegmentUnitToVolumeExtend(VolumeMetadata volume,
      SegmentUnitMetadata newSegUnit) {
    if (newSegUnit.isSecondaryCandidate()) {
      logger.warn("when update extend, the segUnit :{} is candidate, not need save",
          newSegUnit.getSegId());
      return;
    }

    SegmentMetadata segmentInVolume = volume
        .getExtendSegmentByIndex(newSegUnit.getSegId().getIndex());

    if (segmentInVolume == null) { // segment does not exist, create a new
      // one
      SegId segId = new SegId(newSegUnit.getSegId());
      SegmentMetadata newSegment = new SegmentMetadata(segId, newSegUnit.getSegId().getIndex());
      newSegment.putSegmentUnitMetadata(newSegUnit.getInstanceId(), newSegUnit);
      logger.info("Create a newSegment and add a new segment unit to volume: {}", newSegUnit);
      volume.addExtendSegmentMetadata(newSegment, newSegUnit.getMembership());
      // new segment unit comes, put the volume to status transition
      // store.
      volumeStatusStore.addVolumeToStore(volume);
      // put it in the segment unit store to monitor if timeout
      segmentUnitTimeoutStore.addSegmentUnit(newSegUnit);
    } else {
     
     
      SegmentMembership oldMembership = volume.getMembership(segmentInVolume.getIndex());
      SegmentMembership highestMembership = newSegUnit.getMembership();
      SegmentUnitMetadata oldSegUnit = segmentInVolume
          .getSegmentUnitMetadata(newSegUnit.getInstanceId());

      if (oldSegUnit == null) {
       
        segmentUnitTimeoutStore.addSegmentUnit(newSegUnit);
        logger.info("Add a new segment unit to volume: {}", newSegUnit);
        segmentInVolume.putSegmentUnitMetadata(newSegUnit.getInstanceId(), newSegUnit);
        if (segmentInVolume.isSegmentStatusChanged()) {
          
          logger.debug(
              "We put the volume in status store, for the segment status is changed. volume is "
                  + "{}, segment is {}",
              volume, segmentInVolume);
          volumeStatusStore.addVolumeToStore(volume);
        }
      } else {
        // update the content with report segment unit
        if (!oldSegUnit.equals(newSegUnit)) {
          
          oldSegUnit.updateWithNewOne(newSegUnit);
          logger.debug("After update a segment unit: {}", oldSegUnit);
          /* update member ship */
          if (oldMembership == null || oldMembership.compareTo(highestMembership) < 0) {
            volume.addExtendSegmentMetadata(segmentInVolume, highestMembership);
          }

          
          if (segmentInVolume.isSegmentStatusChanged()) {
            logger.debug(
                "put the volume in status store, for the segment status is changed. volume is {},"
                    + " segment is {}",
                volume, segmentInVolume);
            volumeStatusStore.addVolumeToStore(volume);
          }
        }

        oldSegUnit.setMigrationStatus(newSegUnit.getMigrationStatus());
        oldSegUnit.setRatioMigration(newSegUnit.getRatioMigration());
        oldSegUnit.setLastReported(newSegUnit.getLastReported());
      }
    }
  }

  
  private VolumeMetadata createFakeVolume(SegmentUnitMetadata segUnit) {
    VolumeMetadata volumeMetadata = new VolumeMetadata();
    volumeMetadata.setAccountId(SUPERADMIN_ACCOUNT_ID);
    volumeMetadata.setVolumeSize(InfoCenterConstants.volumeSize);
    volumeMetadata.setSegmentSize(InfoCenterConstants.segmentSize);
    volumeMetadata.setRootVolumeId(segUnit.getSegId().getVolumeId().getId());
    // a trick solution, when an expired segment unit meta data come from
    // data node, set the name to the volume id 
    // a constant
    volumeMetadata.setName(segUnit.getSegId().getVolumeId() + InfoCenterConstants.name);
    volumeMetadata.setVolumeStatus(VolumeStatus.Unavailable);
    volumeMetadata.setVolumeId(segUnit.getSegId().getVolumeId().getId());
    volumeMetadata.setVolumeType(segUnit.getVolumeType());
    volumeMetadata.setVolumeSource(VolumeSourceType.CREATE_VOLUME);
    volumeMetadata.setReadWrite(VolumeMetadata.ReadWriteType.READWRITE);
    logger.warn("to createFakeVolume with :{},{}", volumeMetadata.getVolumeId(), volumeMetadata);
    return volumeMetadata;
  }
  /*    report end  **********/

  /*    volume begin  **********/
  private VolumeMetadata processFirstSegmentInVolume(VolumeMetadata volumeMetadata,
      SegmentUnitMetadata segUnit) {

    String volumeMetadataJson = segUnit.getVolumeMetadataJson();
    if (volumeMetadataJson == null) {
      return volumeMetadata;
    }

    VolumeMetadataJsonParser volumeParser = new VolumeMetadataJsonParser(volumeMetadataJson);
    
    if ((volumeMetadata != null) && (volumeMetadata.getName() != null && volumeMetadata.getName()
        .contains(String.valueOf(volumeMetadata.getVolumeId())))) {
      VolumeMetadata volumeFromJson;
      try {
        ObjectMapper mapper = new ObjectMapper();
        volumeFromJson = mapper
            .readValue(volumeParser.getVolumeMetadataJson(), VolumeMetadata.class);
        volumeMetadata.setVolumeSize(volumeFromJson.getVolumeSize());
        volumeMetadata.setName(volumeFromJson.getName());
        volumeMetadata.setVolumeType(volumeFromJson.getVolumeType());
        volumeMetadata.setVolumeLayout(volumeFromJson.getVolumeLayout());
        volumeMetadata
            .setSegmentNumToCreateEachTime(volumeFromJson.getSegmentNumToCreateEachTime());
        volumeMetadata.setDomainId(volumeFromJson.getDomainId());
        volumeMetadata.setStoragePoolId(volumeFromJson.getStoragePoolId());
        volumeMetadata.setSegmentSize(segmentSize);
        volumeMetadata.setVolumeCreatedTime(volumeFromJson.getVolumeCreatedTime());
        volumeMetadata.setLastExtendedTime(volumeFromJson.getLastExtendedTime());
        volumeMetadata.setVolumeSource(volumeFromJson.getVolumeSource());

        /*update the WrappCount and SegmentCount*/
        volumeMetadata.setReadWrite(volumeFromJson.getReadWrite());
        volumeMetadata.setPageWrappCount(volumeFromJson.getPageWrappCount());
        volumeMetadata.setSegmentWrappCount(volumeFromJson.getSegmentWrappCount());
        volumeMetadata.setEachTimeExtendVolumeSize(volumeFromJson.getEachTimeExtendVolumeSize());
        volumeMetadata.setReadWrite(volumeFromJson.getReadWrite());
        // when memory updates, should update volume size, volume name,
        // volume type fields as well
        volumeStore.updateVolumeForReport(volumeMetadata);
        logger.warn("update special volumeMetadata: {}", volumeMetadata);
      } catch (Exception e) {
        logger.error("failed to parse {}", volumeParser.getVolumeMetadataJson(), e);
        return volumeMetadata;
      }
    }

    if (volumeMetadata != null) {
      return volumeMetadata;
    }

    logger
        .warn("when processFirstSegmentInVolume, the volume is null, so get from data node json:{}",
            volumeParser.getVolumeMetadataJson());
    VolumeMetadata volumefromdn;
    try {
      ObjectMapper mapper = new ObjectMapper();
      //add config ignor
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      volumefromdn = mapper.readValue(volumeParser.getVolumeMetadataJson(), VolumeMetadata.class);
      volumefromdn.setSegmentSize(segmentSize);
    } catch (Exception e) {
      logger.error("failed to parse {}", volumeParser.getVolumeMetadataJson(), e);
      return volumeMetadata;
    }

    boolean volumeMetadataUpdateFlag = false;
    if (volumeMetadata == null) {
      logger.warn("volumeMetadata is null, i will produce new volume metadata volume json: {}",
          volumeParser.getVolumeMetadataJson());
      volumeMetadata = volumefromdn;
      volumeMetadata.setVolumeStatus(VolumeStatus.Unavailable);
      volumeMetadataUpdateFlag = true;
    }

    volumeStore.saveVolumeForReport(volumeMetadata);
    logger.warn("volumeMetadata from datanode : {}", volumefromdn);
    return volumeMetadata;
  }

  
  @Override
  public synchronized CreateVolumeResponse createVolume(CreateVolumeRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      AccountNotFoundExceptionThrift,
      PermissionNotGrantExceptionThrift, AccessDeniedExceptionThrift, InvalidInputExceptionThrift,
      VolumeSizeNotMultipleOfSegmentSizeThrift, NotEnoughSpaceExceptionThrift,
      InvalidInputExceptionThrift,
      VolumeNameExistedExceptionThrift, VolumeExistingExceptionThrift,
      StoragePoolNotExistInDoaminExceptionThrift, DomainNotExistedExceptionThrift,
      StoragePoolNotExistedExceptionThrift, DomainIsDeletingExceptionThrift,
      StoragePoolIsDeletingExceptionThrift, NotEnoughGroupExceptionThrift,
      TException {

    //check the instance status
    checkInstanceStatus("createVolume");

    logger.warn("createVolume request: {}", request);
    securityManager.hasPermission(request.getAccountId(), "createVolume");
    securityManager.hasRightToAccess(request.getAccountId(), request.getDomainId());
    securityManager.hasRightToAccess(request.getAccountId(), request.getStoragePoolId());

    String volumeName = request.getName();
    if (volumeName == null || volumeName.trim().length() > 100) {
      logger.error("Volume name is null or too long, exceed 100");
      throw new InvalidInputExceptionThrift();
    }

    String volumeDescription = request.getVolumeDescription();
    logger.warn("Volume description is too long :{}", volumeDescription);
    if (volumeDescription != null && volumeDescription.trim().length() > 128) {
      logger.error("Volume description is too long, exceed 128");
      throw new InvalidInputExceptionThrift();
    }

    request.setName(volumeName.trim());

    //get fact create volume size
    long volumeSize = ceilVolumeSizeWithSegmentSize(request.getVolumeSize());
    if (volumeSize % segmentSize != 0 || volumeSize < segmentSize) {
      logger
          .error("volume size is not multiple of segment size {}, volume size is {} ", segmentSize,
              volumeSize);
      throw new VolumeSizeNotMultipleOfSegmentSizeThrift(segmentSize);
    }

    //set volume size
    request.setVolumeSize(volumeSize);

    VolumeCreationRequest volumeRequest = RequestResponseHelper
        .buildCreateVolumeRequest(segmentSize, request);
    volumeRequest.setStatus(VolumeStatus.ToBeCreated.name());

    /* create VolumeMetadata and save to db  **/
    makeVolumeCreationRequestAndcreateVolume(volumeRequest);
    volumeJobStoreDb.saveCreateOrExtendVolumeRequest(volumeRequest);
    logger.warn("Successfully saved volume creation request: {}", volumeRequest.getVolumeId());

    buildActiveOperationAndSaveToDb(volumeRequest.getAccountId(), volumeRequest.getVolumeId(),
        OperationType.CREATE,
        TargetType.VOLUME, "", volumeRequest.getName(), 0L);

    CreateVolumeResponse response = new CreateVolumeResponse();
    response.setVolumeId(volumeRequest.getVolumeId());
    response.setRequestId(request.getRequestId());
    PyResource volumeResource = new PyResource(volumeRequest.getVolumeId(), request.getName(),
        PyResource.ResourceType.Volume.name());
    securityManager.saveResource(volumeResource);
    securityManager.addResource(request.getAccountId(), volumeResource);

    logger.warn("createVolume response: {}", response);
    return response;
  }

  @Override
  public ExtendVolumeResponse extendVolume(ExtendVolumeRequest request)
      throws ServiceIsNotAvailableThrift, AccountNotFoundExceptionThrift,
      PermissionNotGrantExceptionThrift,
      ServiceHavingBeenShutdownThrift, AccessDeniedExceptionThrift,
      VolumeSizeNotMultipleOfSegmentSizeThrift,
      RootVolumeNotFoundExceptionThrift, RootVolumeBeingDeletedExceptionThrift,
      VolumeOriginalSizeNotMatchExceptionThrift,
      VolumeNotAvailableExceptionThrift, VolumeIsCopingExceptionThrift,
      VolumeIsCloningExceptionThrift,
      VolumeInMoveOnlineDoNotHaveOperationExceptionThrift, VolumeInExtendingExceptionThrift,
      VolumeWasRollbackingExceptionThrift, NotEnoughSpaceExceptionThrift,
      InvalidInputExceptionThrift,
      VolumeNameExistedExceptionThrift, VolumeExistingExceptionThrift,
      StoragePoolNotExistInDoaminExceptionThrift, DomainNotExistedExceptionThrift,
      StoragePoolNotExistedExceptionThrift, DomainIsDeletingExceptionThrift,
      StoragePoolIsDeletingExceptionThrift, VolumeDeletingExceptionThrift,
      VolumeIsBeginMovedExceptionThrift,
      VolumeCyclingExceptionThrift, VolumeIsMovingExceptionThrift, TException {

    //check the instance status
    checkInstanceStatus("extendVolume");
    logger.warn("extendVolume request: {}", request);

    long accountId = request.getAccountId();
    long volumeId = request.getVolumeId();
    securityManager.hasPermission(accountId, "extendVolume");
    securityManager.hasRightToAccess(accountId, volumeId);

    //get fact extend volume size
    long extendSize = ceilVolumeSizeWithSegmentSize(request.getExtendSize());
    if (extendSize % segmentSize != 0 || extendSize < segmentSize) {
      logger.error("volume size is not integral multiple of segment size: {} extend size: {}",
          segmentSize,
          extendSize);
      throw new VolumeSizeNotMultipleOfSegmentSizeThrift(segmentSize);
    }

    //set extend size
    request.setExtendSize(extendSize);

    // volume is in copy status, we cannot extend it
    VolumeMetadata volumeToExtend = null;
    List<DriverMetadata> driverMetadataList = null;
    VolumeMetadataAndDrivers volumeAndDriverInfo = null;
    try {
      volumeAndDriverInfo = volumeInformationManger
          .getDriverContainVolumes(volumeId, request.getAccountId(), true);
      volumeToExtend = volumeAndDriverInfo.getVolumeMetadata();
      driverMetadataList = volumeAndDriverInfo.getDriverMetadatas();
    } catch (VolumeNotFoundException e) {
      logger.error("when extend volume, can not find this volume: {}", volumeId);
      throw new RootVolumeNotFoundExceptionThrift();
    }

    //check the Operation
    checkExceptionForOperation(volumeToExtend, OperationFunctionType.extendVolume);

    if (volumeToExtend.isDeletedByUser()) {
      logger.error("when extend volume, the volume: {} is Deleted By User", volumeId);
      throw new RootVolumeBeingDeletedExceptionThrift();
    }

    if (!volumeToExtend.isVolumeAvailable()) {
      logger.error("when extend volume, the volume: {} is NotAvailable", volumeId);
      throw new VolumeNotAvailableExceptionThrift();
    }

    LockForSaveVolumeInfo.VolumeLockInfo lock = lockForSaveVolumeInfo
        .getVolumeLockInfo(volumeId, LockForSaveVolumeInfo.BusySource.VOLUME_EXTEND);

    try {
      //for save to db
      VolumeMetadata volumeToExtendToSave = volumeStore.getVolume(volumeId);
      if (volumeToExtendToSave == null) {
        logger.error("when extend volume, can not find this volume: {}", volumeId);
        throw new RootVolumeNotFoundExceptionThrift();
      }
      if (volumeToExtendToSave.getVolumeSize() != request.getOriginalSize()) {
        logger.error("when extend volume, the volume : {} has been extended", volumeId);
        throw new VolumeOriginalSizeNotMatchExceptionThrift();
      }
      VolumeCreationRequest volumeRequest = RequestResponseHelper
          .buildExtendVolumeRequest(segmentSize, volumeToExtendToSave, request.getExtendSize());
      volumeRequest.setStatus(VolumeStatus.ToBeCreated.name());

      //check extend info
      makeVolumeCreationRequestAndcreateVolume(volumeRequest);

      /* set the extend volume info **/
      volumeToExtendToSave.setExtendingSize(request.getExtendSize());
      volumeToExtendToSave.setLastExtendedTime(new Date());
      volumeToExtendToSave.setInAction(EXTENDING);
      volumeToExtendToSave.setPageWrappCount(pageWrappCount);
      volumeToExtendToSave.setSegmentWrappCount(segmentWrappCount);
      volumeStore.saveVolume(volumeToExtendToSave); // update root volume to db
      logger.warn("Successfully saved extend volume info: {}", volumeToExtendToSave);

      //save extend volume request
      volumeJobStoreDb.saveCreateOrExtendVolumeRequest(volumeRequest);

      Set<EndPoint> bindedCoordinatorEps = new HashSet<EndPoint>();
      for (int tryCount = 0; tryCount < 3; tryCount++) {
        for (DriverMetadata driver : driverMetadataList) {
          bindedCoordinatorEps.add(new EndPoint(driver.getHostName(), driver.getCoordinatorPort()));
        }

        logger.warn("bindedCoordinatorEps:{}", bindedCoordinatorEps);
        Set<Instance> coordinators = instanceStore
            .getAll(PyService.COORDINATOR.getServiceName(), InstanceStatus.HEALTHY);
        for (Instance coordinator : coordinators) {
          EndPoint ioEndpoint = coordinator.getEndPointByServiceName(PortType.IO);
          logger.warn("get coordinator endPoint:{}", ioEndpoint);
          if (!bindedCoordinatorEps.contains(ioEndpoint)) {
            logger.warn("didn't contain coordinator:{}", ioEndpoint);
            continue;
          }

          EndPoint endpoint = coordinator.getEndPoint();
          try {
            CoordinatorClientFactory.CoordinatorClientWrapper coordinatorClientWrapper =
                coordinatorClientFactory
                    .build(endpoint);
            UpdateVolumeOnExtendingRequest updateVolumeOnExtendingRequest =
                new UpdateVolumeOnExtendingRequest(
                    RequestIdBuilder.get(), volumeId);
            logger.warn(
                "going to send update volume extending request to coordinator: {}, and the "
                    + "updateVolumeOnExtendingRequest",
                endpoint, updateVolumeOnExtendingRequest);
            coordinatorClientWrapper.getClient()
                .updateVolumeOnExtending(updateVolumeOnExtendingRequest);
          } catch (GenericThriftClientFactoryException e) {
            logger.error("Caught an exception when build coordinator client {}", endpoint, e);
            continue;
          } catch (Exception e) {
            logger.error(
                "Caught an exception when send request to update volume {} info in coordinator {}",
                volumeId, endpoint, e);
            continue;
          }

          bindedCoordinatorEps.remove(ioEndpoint);
        }

        if (!bindedCoordinatorEps.isEmpty()) {
          logger.warn("Unable to notify coordinators on {} the extending volume event",
              bindedCoordinatorEps);
          try {
            Thread.sleep(5000);
          } catch (InterruptedException e) {
            logger.warn("sleep interrupted!");
          }
        } else {
          break;
        }
      }

      // begin add extend-volume operation
      buildActiveOperationAndSaveToDb(request.getAccountId(), volumeToExtendToSave.getVolumeId(),
          OperationType.EXTEND, TargetType.VOLUME, volumeToExtendToSave.getName(), "",
          volumeToExtendToSave.getVolumeSize());
    } finally {
      lock.setVolumeOperation(LockForSaveVolumeInfo.BusySource.LOCK_WAITING);
      lock.unlock();
    }

    ExtendVolumeResponse response = new ExtendVolumeResponse();
    response.setRequestId(request.getRequestId());
    return response;
  }

  @Override
  public GetSegmentListResponse getSegmentList(GetSegmentListRequest request)
      throws InternalErrorThrift, InvalidInputExceptionThrift, NotEnoughSpaceExceptionThrift,
      VolumeNotFoundExceptionThrift, ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      TException {

    //check the instance status
    checkInstanceStatus("getSegmentList");

    logger.warn("getSegmentList request: {}", request);
    long volumeId = request.getVolumeId();

    VolumeMetadata volumeMetadata;
    try {
      volumeMetadata = volumeInformationManger.getVolumeNew(volumeId, request.getAccountId());
    } catch (VolumeNotFoundException e) {
      logger.error("when getSegmentList, can not find the volume :{}", volumeId);
      throw new VolumeNotFoundExceptionThrift();
    }

    GetSegmentListResponse response = new GetSegmentListResponse();
    response.setSegments(RequestResponseHelper
        .buildThriftSegmentListFrom(volumeMetadata, request.getStartSegmentIndex(),
            request.getEndSegmentIndex()));

    response.setRequestId(request.getRequestId());
    logger.info("getSegmentList response: {}", response);

    return response;
  }

  @Override
  public py.thrift.icshare.DeleteVolumeResponse deleteVolume(
      py.thrift.icshare.DeleteVolumeRequest request)
      throws VolumeNotFoundExceptionThrift, AccessDeniedExceptionThrift,
      InvalidInputExceptionThrift,
      VolumeBeingDeletedExceptionThrift, LaunchedVolumeCannotBeDeletedExceptionThrift,
      VolumeInExtendingExceptionThrift, VolumeWasRollbackingExceptionThrift,
      SnapshotRollingBackExceptionThrift,
      DriverLaunchingExceptionThrift,
      DriverUnmountingExceptionThrift, VolumeDeletingExceptionThrift,
      VolumeIsCloningExceptionThrift,
      PermissionNotGrantExceptionThrift, AccountNotFoundExceptionThrift,
      ServiceHavingBeenShutdownThrift,
      ServiceIsNotAvailableThrift, NotEnoughSpaceExceptionThrift, VolumeIsCopingExceptionThrift,
      ExistsDriverExceptionThrift, VolumeIsBeginMovedExceptionThrift, VolumeIsMovingExceptionThrift,
      VolumeNotAvailableExceptionThrift, VolumeInMoveOnlineDoNotHaveOperationExceptionThrift,
      VolumeCyclingExceptionThrift, TException {

    //check the instance status
    checkInstanceStatus("deleteVolume");

    logger.warn("deleteVolume request: {}", request);
    long volumeId = request.getVolumeId();
    securityManager.hasPermission(request.getAccountId(), "deleteVolume");
    securityManager.hasRightToAccess(request.getAccountId(), volumeId);

    VolumeMetadata volume;
    try {
      volume = volumeInformationManger.getVolumeNew(volumeId, request.getAccountId());
    } catch (VolumeNotFoundException e) {
      logger.error("when deleteVolume, can not find the volume :{}", volumeId);
      throw new VolumeNotFoundExceptionThrift();
    }

    //check the Operation
    checkExceptionForOperation(volume, OperationFunctionType.deleteVolume);

    boolean markVolumeDelete = false;

    // volume not in deleting or deleted status
    if (volume.isDeletedByUser() || volume.isMarkDelete()) {
      logger.warn("volume :{} is being deleting or deleted", volume.getVolumeId());
      throw new VolumeBeingDeletedExceptionThrift();
    }

    LockForSaveVolumeInfo.VolumeLockInfo lock = lockForSaveVolumeInfo
        .getVolumeLockInfo(volumeId, LockForSaveVolumeInfo.BusySource.VOLUME_DELETING);
    try {
      // check if the volume has been launched
      List<DriverMetadata> volumeBindingDrivers = driverStore.get(volumeId);
      if (volumeBindingDrivers.size() > 0) {
        LaunchedVolumeCannotBeDeletedExceptionThrift launchedVolumeCannotBeDeletedExceptionThrift =
            new LaunchedVolumeCannotBeDeletedExceptionThrift();
        launchedVolumeCannotBeDeletedExceptionThrift.setIsDriverUnknown(false);
        for (DriverMetadata driverMetadata : volumeBindingDrivers) {
          if (driverMetadata.getDriverStatus() == DriverStatus.UNKNOWN) {
            launchedVolumeCannotBeDeletedExceptionThrift.setIsDriverUnknown(true);
          }
        }
        logger.warn("volume has been launched, deleting operation not allowed");
        throw launchedVolumeCannotBeDeletedExceptionThrift;
      }

      // get the list of volume metadata first, and set all the volume is deleting status
      if (markVolumeDelete) {
        volumeStore.markVolumeDelete(volumeId);
      } else {
        volume.setVolumeStatus(VolumeStatus.Deleting);
      }

      if (!markVolumeDelete) {
        volumeStore.updateStatusAndVolumeInAction(volumeId, VolumeStatus.Deleting.toString(),
            DELETING.name());
        logger.warn("user delete volume {}, volume status change to {}", volume,
            VolumeStatus.Deleting);

        //check the new volume from Move volume
        List<DeleteVolumeRequest> deleteVolumeRequests = volumeJobStoreDb.getDeleteVolumeRequest();
        if (deleteVolumeRequests.size() > 0) {
          //check the volume
          for (DeleteVolumeRequest deleteVolumeRequest : deleteVolumeRequests) {
            long newVolumeId = deleteVolumeRequest.getNewVolumeId();
            long srcVolumeId = deleteVolumeRequest.getVolumeId();
            String volumeName = deleteVolumeRequest.getVolumeName();
            String newVolumeName = deleteVolumeRequest.getNewVolumeName();
            if (volumeId == newVolumeId) {
              logger.warn(
                  "delete volume:{} {}, find it is Move volume new volume, so delete the src "
                      + "volume:{} {}",
                  newVolumeId, newVolumeName, srcVolumeId, volumeName);

              volumeStore
                  .updateStatusAndVolumeInAction(srcVolumeId, VolumeStatus.Deleting.toString(),
                      DELETING.name());
              volumeJobStoreDb.deleteDeleteVolumeRequest(deleteVolumeRequest);
              logger
                  .warn("delete the src volume:{} {} for Move volume ok", srcVolumeId, volumeName);
            }
          }
        }
      }

      /*  delete volume access rule related to the volume **/
      volumeRuleRelationshipStore.deleteByVolumeId(volumeId);

      /*  delete volume in delete and Recycle **/
      volumeDelayStore.deleteVolumeDelayInfo(volumeId);

      // begin add delete-volume operation
      if (request.getVolumeName() != null) {
        long operationId = buildActiveOperationAndSaveToDb(request.getAccountId(), volumeId,
            OperationType.DELETE,
            TargetType.VOLUME, request.getVolumeName(), "", 0L);

        if (markVolumeDelete) {
          // save operation to db
          Operation operation = operationStore.getOperation(operationId);
          operation.setProgress(100L);
          operation.setStatus(OperationStatus.SUCCESS);
          operation.setEndTime(System.currentTimeMillis());
          logger.warn("when delete src clone volume, end operation right now, operation: {}",
              operation);
          operationStore.saveOperation(operation);
        }
      }
    } catch (LaunchedVolumeCannotBeDeletedExceptionThrift e) {
      logger.error("Caught an exception", e);
      throw e;
    } catch (Exception e) {
      logger.error("Caught an exception", e);
      throw new TException(e);
    } finally {
      lock.setVolumeOperation(LockForSaveVolumeInfo.BusySource.LOCK_WAITING);
      lock.unlock();
    }

    lockForSaveVolumeInfo.removeLockByVolumeId(volumeId);
    DeleteVolumeResponse response = new DeleteVolumeResponse(request.getRequestId());
    logger.warn("deleteVolume response: {}", response);
    return response;
  }

  @Override
  public UpdateVolumeDescriptionResponse updateVolumeDescription(
      UpdateVolumeDescriptionRequest request)
      throws AccessDeniedExceptionThrift, VolumeNotFoundExceptionThrift,
      VolumeBeingDeletedExceptionThrift,
      ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {
    checkInstanceStatus("updateVolumeDescription");
    logger.warn("updateVolumeDescription request: {}", request);

    long volumeId = request.getVolumeId();
    //check volume
    VolumeMetadata volumeMetadataCheck = volumeStore.getVolume(volumeId);
    if (volumeMetadataCheck == null) {
      logger.warn("updateVolumeDescription for volume:{}, can to find the volume", volumeId);
      throw new VolumeNotFoundExceptionThrift();
    }

    volumeStore.updateDescription(volumeId, request.getVolumeDescription());

    UpdateVolumeDescriptionResponse response = new UpdateVolumeDescriptionResponse(
        request.getRequestId());
    logger.warn("updateVolumeDescription response: {}", response);
    return response;
  }

  @Override
  public DeleteVolumeDelayResponse deleteVolumeDelay(DeleteVolumeDelayRequest request)
      throws AccessDeniedExceptionThrift, VolumeNotFoundExceptionThrift,
      VolumeBeingDeletedExceptionThrift, ServiceHavingBeenShutdownThrift,
      ServiceIsNotAvailableThrift, TException {
    checkInstanceStatus("deleteVolumeDelay");
    logger.warn("deleteVolumeDelay request: {}", request);

    long volumeId = request.getVolumeId();
    //check volume
    VolumeMetadata volumeMetadataCheck = volumeStore.getVolume(volumeId);
    if (volumeMetadataCheck == null) {
      logger.warn("deleteVolumeDelay for volume:{}, can to find the volume", volumeId);
      throw new VolumeNotFoundExceptionThrift();
    }

    //check
    VolumeDeleteDelayInformation volumeDeleteDelayInformationCheck = volumeDelayStore
        .getVolumeDelayInfo(volumeId);
    if (volumeDeleteDelayInformationCheck != null) {
      logger.warn("deleteVolumeDelay for volume:{}, this volume in delete delay", volumeId);
      throw new TException();
    }

    VolumeDeleteDelayInformation volumeDeleteDelayInformation = new VolumeDeleteDelayInformation();
    volumeDeleteDelayInformation.setVolumeId(volumeId);
    volumeDeleteDelayInformation.setTimeForDelay(TimeUnit.DAYS.toSeconds(request.getDelaydate()));
    volumeDeleteDelayInformation.setStopDelay(false);

    volumeDelayStore.saveVolumeDelayInfo(volumeDeleteDelayInformation);
    DeleteVolumeDelayResponse response = new DeleteVolumeDelayResponse(request.getRequestId());
    logger.warn("deleteVolumeDelay response: {}", response);
    return response;
  }

  @Override
  public StopDeleteVolumeDelayResponse stopDeleteVolumeDelay(StopDeleteVolumeDelayRequest request)
      throws AccessDeniedExceptionThrift, VolumeNotFoundExceptionThrift,
      VolumeBeingDeletedExceptionThrift,
      ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {
    checkInstanceStatus("stopDeleteVolumeDelay");
    logger.warn("stopDeleteVolumeDelay request: {}", request);

    long volumeId = request.getVolumeId();
    //check volume
    VolumeMetadata volumeMetadataCheck = volumeStore.getVolume(volumeId);
    if (volumeMetadataCheck == null) {
      logger.warn("stopDeleteVolumeDelay for volume:{}, can to find the volume", volumeId);
      throw new VolumeNotFoundExceptionThrift();
    }

    //check
    VolumeDeleteDelayInformation volumeDeleteDelayInformationCheck = volumeDelayStore
        .getVolumeDelayInfo(volumeId);
    if (volumeDeleteDelayInformationCheck == null) {
      logger.warn("stopDeleteVolumeDelay for volume:{}, cat not find the volume in delete delay",
          volumeId);
      throw new TException();
    }

    volumeDelayStore.updateVolumeDelayStatusInfo(volumeId, true);
    StopDeleteVolumeDelayResponse response = new StopDeleteVolumeDelayResponse(
        request.getRequestId());
    logger.warn("stopDeleteVolumeDelay response: {}", response);
    return response;
  }

  @Override
  public StartDeleteVolumeDelayResponse startDeleteVolumeDelay(
      StartDeleteVolumeDelayRequest request)
      throws AccessDeniedExceptionThrift, VolumeNotFoundExceptionThrift,
      VolumeBeingDeletedExceptionThrift,
      ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {
    checkInstanceStatus("startDeleteVolumeDelay");
    logger.warn("startDeleteVolumeDelay request: {}", request);
    long volumeId = request.getVolumeId();
    //check volume
    VolumeMetadata volumeMetadataCheck = volumeStore.getVolume(volumeId);
    if (volumeMetadataCheck == null) {
      logger.warn("startDeleteVolumeDelay for volume:{}, can to find the volume", volumeId);
      throw new VolumeNotFoundExceptionThrift();
    }

    //check
    VolumeDeleteDelayInformation volumeDeleteDelayInformationCheck = volumeDelayStore
        .getVolumeDelayInfo(volumeId);
    if (volumeDeleteDelayInformationCheck == null) {
      logger.warn("startDeleteVolumeDelay for volume:{}, cat not find the volume in delete delay",
          volumeId);
      throw new TException();
    }

    volumeDelayStore.updateVolumeDelayStatusInfo(volumeId, false);
    StartDeleteVolumeDelayResponse response = new StartDeleteVolumeDelayResponse(
        request.getRequestId());
    logger.warn("startDeleteVolumeDelay response: {}", response);
    return response;
  }

  @Override
  public GetDeleteVolumeDelayResponse getDeleteVolumeDelay(GetDeleteVolumeDelayRequest request)
      throws AccessDeniedExceptionThrift, VolumeNotFoundExceptionThrift,
      VolumeBeingDeletedExceptionThrift,
      ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {
    checkInstanceStatus("getDeleteVolumeDelay");
    logger.warn("getDeleteVolumeDelay request: {}", request);

    long volumeId = request.getVolumeId();
    //check volume
    VolumeMetadata volumeMetadataCheck = volumeStore.getVolume(volumeId);
    if (volumeMetadataCheck == null) {
      logger.warn("getDeleteVolumeDelay for volume:{}, can to find the volume", volumeId);
      throw new VolumeNotFoundExceptionThrift();
    }

    //check
    VolumeDeleteDelayInformation volumeDeleteDelayInformationCheck = volumeDelayStore
        .getVolumeDelayInfo(volumeId);
    if (volumeDeleteDelayInformationCheck == null) {
      logger.warn("getDeleteVolumeDelay for volume:{}, cat not find the volume in delete delay",
          volumeId);
      throw new TException();
    }

    GetDeleteVolumeDelayResponse response = new GetDeleteVolumeDelayResponse(request.getRequestId(),
        RequestResponseHelper.buildThriftVolumeDeleteDelayFrom(volumeDeleteDelayInformationCheck));
    logger.warn("getDeleteVolumeDelay response: {}", response);
    return response;
  }

  @Override
  public CancelDeleteVolumeDelayResponse cancelDeleteVolumeDelay(
      CancelDeleteVolumeDelayRequest request)
      throws AccessDeniedExceptionThrift, VolumeNotFoundExceptionThrift,
      ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {
    checkInstanceStatus("cancelDeleteVolumeDelay");
    logger.warn("cancelDeleteVolumeDelay request: {}", request);

    long volumeId = request.getVolumeId();
    //check volume
    VolumeMetadata volumeMetadataCheck = volumeStore.getVolume(volumeId);
    if (volumeMetadataCheck == null) {
      logger.warn("cancelDeleteVolumeDelay for volume:{}, can to find the volume", volumeId);
      throw new VolumeNotFoundExceptionThrift();
    }

    //check
    VolumeDeleteDelayInformation volumeDeleteDelayInformationCheck = volumeDelayStore
        .getVolumeDelayInfo(volumeId);
    if (volumeDeleteDelayInformationCheck == null) {
      logger.warn("cancelDeleteVolumeDelay for volume:{}, cat not find the volume in delete delay",
          volumeId);
      throw new TException();
    }

    volumeDelayStore.deleteVolumeDelayInfo(volumeId);
    CancelDeleteVolumeDelayResponse response = new CancelDeleteVolumeDelayResponse(
        request.getRequestId());
    logger.warn("cancelDeleteVolumeDelay response: {}", response);
    return response;
  }

  @Override
  public MoveVolumeToRecycleResponse moveVolumeToRecycle(MoveVolumeToRecycleRequest request)
      throws AccessDeniedExceptionThrift, VolumeNotFoundExceptionThrift,
      VolumeBeingDeletedExceptionThrift,
      ServiceHavingBeenShutdownThrift, VolumeInExtendingExceptionThrift,
      LaunchedVolumeCannotBeDeletedExceptionThrift,
      ServiceIsNotAvailableThrift, DriverLaunchingExceptionThrift, DriverUnmountingExceptionThrift,
      VolumeDeletingExceptionThrift, VolumeWasRollbackingExceptionThrift,
      VolumeIsCloningExceptionThrift, PermissionNotGrantExceptionThrift,
      AccountNotFoundExceptionThrift,
      VolumeIsCopingExceptionThrift, VolumeInMoveOnlineDoNotHaveOperationExceptionThrift,
      VolumeIsBeginMovedExceptionThrift,
      VolumeIsMovingExceptionThrift, TException {
    checkInstanceStatus("moveVolumeToRecycle");
    logger.warn("moveVolumeToRecycle request: {}", request);
    long volumeId = request.getVolumeId();
    //check volume
    VolumeMetadata volumeMetadataCheck = volumeStore.getVolume(volumeId);
    if (volumeMetadataCheck == null) {
      logger.warn("moveVolumeToRecycle for volume:{}, can to find the volume", volumeId);
      throw new VolumeNotFoundExceptionThrift();
    }

    volumeRecycleManager.checkVolumeInfo(volumeId);

    //delete in volumeDelayStore
    VolumeDeleteDelayInformation volumeDeleteDelayInformationCheck = volumeDelayStore
        .getVolumeDelayInfo(volumeId);
    if (volumeDeleteDelayInformationCheck != null) {
      logger.warn("moveVolumeToRecycle for volume:{}, find the volume in delete delay, remove it",
          volumeId);
      volumeDelayStore.deleteVolumeDelayInfo(volumeId);
    }

    //remove to Recycle db
    VolumeRecycleInformation volumeRecycleInformation = new VolumeRecycleInformation(volumeId,
        System.currentTimeMillis());
    volumeRecycleStore.saveVolumeRecycleInfo(volumeRecycleInformation);

    MoveVolumeToRecycleResponse response = new MoveVolumeToRecycleResponse(request.getRequestId());
    logger.warn("moveVolumeToRecycle response: {}", response);
    return response;
  }

  @Override
  public ListRecycleVolumeInfoResponse listRecycleVolumeInfo(ListRecycleVolumeInfoRequest request)
      throws AccessDeniedExceptionThrift, ServiceHavingBeenShutdownThrift,
      ServiceIsNotAvailableThrift, TException {
    checkInstanceStatus("listRecycleVolumeInfo");
    logger.warn("listRecycleVolumeInfo request: {}", request);

    //list volume
    List<VolumeMetadata> volumeMetadataList = volumeStore.listVolumes();
    Map<Long, VolumeMetadata> volumeMetadataMap = new HashMap<>();
    for (VolumeMetadata volumeMetadata : volumeMetadataList) {
      if (volumeMetadata.getVolumeStatus().equals(VolumeStatus.Dead)) {
        continue;
      }

      volumeMetadataMap.put(volumeMetadata.getVolumeId(), volumeMetadata);
    }

    Map<Long, VolumeRecycleInformationThrift> volumeRecycleInformationThrifts = new HashMap<>();
    List<VolumeMetadataThrift> volumeMetadataThriftList = new LinkedList<>();
    List<VolumeRecycleInformation> volumeRecycleInformationList = volumeRecycleStore
        .listVolumesRecycleInfo();
    for (VolumeRecycleInformation volumeRecycleInformation : volumeRecycleInformationList) {
      long volumeId = volumeRecycleInformation.getVolumeId();
      VolumeMetadata volumeMetadata = volumeMetadataMap.get(volumeId);
      if (volumeMetadata == null) {
        logger.warn("for listRecycleVolumeInfo in volume :{}, can not find the volume", volumeId);
        continue;
      } else {
        volumeMetadataThriftList
            .add(RequestResponseHelper.buildThriftVolumeFrom(volumeMetadata, false));
        volumeRecycleInformationThrifts.put(volumeId, RequestResponseHelper
            .buildThriftVolumeRecycleInformationFrom(volumeRecycleInformation));
      }
    }

    ListRecycleVolumeInfoResponse response = new ListRecycleVolumeInfoResponse(
        request.getRequestId(), volumeMetadataThriftList,
        volumeRecycleInformationThrifts);
    logger.warn("listRecycleVolumeInfo response: {}", response);
    return response;
  }

  @Override
  public RecycleVolumeToNormalResponse recycleVolumeToNormal(RecycleVolumeToNormalRequest request)
      throws AccessDeniedExceptionThrift, VolumeNotFoundExceptionThrift,
      VolumeBeingDeletedExceptionThrift,
      ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {
    checkInstanceStatus("recycleVolumeToNormal");
    logger.warn("recycleVolumeToNormal request: {}", request);

    long volumeId = request.getVolumeId();
    //check volume
    VolumeMetadata volumeMetadataCheck = volumeStore.getVolume(volumeId);
    if (volumeMetadataCheck == null) {
      logger.warn("recycleVolumeToNormal for volume:{}, can to find the volume", volumeId);
      throw new VolumeNotFoundExceptionThrift();
    }

    //check
    VolumeRecycleInformation volumeRecycleInformationGet = volumeRecycleStore
        .getVolumeRecycleInfo(volumeId);
    if (volumeRecycleInformationGet == null) {
      logger.warn("recycleVolumeToNormal for volume:{}, cat not find the volume in delete delay",
          volumeId);
    } else {
      volumeRecycleStore.deleteVolumeRecycleInfo(volumeId);
    }

    RecycleVolumeToNormalResponse response = new RecycleVolumeToNormalResponse(
        request.getRequestId());
    logger.warn("recycleVolumeToNormal response: {}", response);
    return response;
  }

  @Override
  public RecycleVolumeResponse recycleVolume(RecycleVolumeRequest request)
      throws AccessDeniedExceptionThrift, VolumeNotFoundExceptionThrift,
      VolumeCannotBeRecycledExceptionThrift,
      ServiceHavingBeenShutdownThrift, VolumeInExtendingExceptionThrift,
      ExistsDriverExceptionThrift,
      ServiceIsNotAvailableThrift, PermissionNotGrantExceptionThrift,
      AccountNotFoundExceptionThrift,
      NotEnoughSpaceExceptionThrift, InvalidInputExceptionThrift,
      VolumeWasRollbackingExceptionThrift,
      VolumeDeletingExceptionThrift, VolumeCyclingExceptionThrift,
      VolumeNotAvailableExceptionThrift,
      VolumeIsBeginMovedExceptionThrift, VolumeIsCloningExceptionThrift,
      VolumeIsMovingExceptionThrift,
      VolumeInMoveOnlineDoNotHaveOperationExceptionThrift, VolumeIsCopingExceptionThrift,
      TException {

    //check the instance status
    checkInstanceStatus("recycleVolume");
    logger.warn("recycleVolume request: {}", request);

    securityManager.hasPermission(request.getAccountId(), "recycleVolume");
    securityManager.hasRightToAccess(request.getAccountId(), request.getVolumeId());

    VolumeMetadata volume;
    long volumeId = request.getVolumeId();
    try {
      volume = volumeInformationManger.getVolumeNew(volumeId, request.getAccountId());
    } catch (VolumeNotFoundException e) {
      logger.error("when recycleVolume, can not find the volume :{}", volumeId);
      throw new VolumeNotFoundExceptionThrift();
    }

    //check the Operation
    checkExceptionForOperation(volume, OperationFunctionType.recycleVolume);

    LockForSaveVolumeInfo.VolumeLockInfo lock = lockForSaveVolumeInfo
        .getVolumeLockInfo(volumeId, LockForSaveVolumeInfo.BusySource.VOLUME_CYCLING);

    try {
      // volume not in deleting or deleted status
      if (!volume.canBeRecycled()) {
        logger.warn("volume cannot be recycled {} ", volume);
        throw new VolumeCannotBeRecycledExceptionThrift();
      }

      /* when recycleVolume ,check the volume status*/
      VolumeStatus volumeStatus = volume.getVolumeStatus();
      if (volumeStatus == VolumeStatus.Dead) {
        logger.warn("when recycleVolume :{}, the volume is Dead", volumeId);
        throw new VolumeCannotBeRecycledExceptionThrift();
      }

      // set the volume status to deleting;
      volume.setVolumeStatus(VolumeStatus.Recycling);
      volumeStore.updateStatusAndVolumeInAction(volumeId, VolumeStatus.Recycling.toString(),
          RECYCLING.name());
      logger.debug("user delete volume {}, volume status change to {}", volume,
          VolumeStatus.Recycling);

      // begin add delete-volume operation
      logger.debug("begin save delete-volume operation");
      buildActiveOperationAndSaveToDb(request.getAccountId(), volumeId, OperationType.CYCLE,
          TargetType.VOLUME,
          volume.getName(), "", 0L);
    } finally {
      lock.setVolumeOperation(LockForSaveVolumeInfo.BusySource.LOCK_WAITING);
      lock.unlock();
    }

    RecycleVolumeResponse response = new RecycleVolumeResponse(request.getRequestId());
    logger.warn("recycleVolume response: {}", response);
    return response;
  }

  @Override
  public CreateSegmentsResponse createSegments(
      py.thrift.infocenter.service.CreateSegmentsRequest request)
      throws NotEnoughSpaceExceptionThrift, SegmentExistingExceptionThrift, NoMemberExceptionThrift,
      ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, NetworkErrorExceptionThrift,
      TException {

    //check the instance status
    checkInstanceStatus("createSegments");
    logger.warn("createSegment request: {}", request);

    long volumeId = request.getVolumeId();
    int startSegmentIndex = request.getSegIndex();
    int newSegmentsCount = request.getNumToCreate();

    VolumeMetadata volumeMetadata = volumeStore.getVolume(volumeId);
    if (volumeMetadata == null) {
      logger.warn("when createSegments, can not find the volume:{}", volumeId);
      throw new VolumeNotFoundExceptionThrift();
    }

    LockForSaveVolumeInfo.VolumeLockInfo lock = lockForSaveVolumeInfo
        .getVolumeLockInfo(volumeId, LockForSaveVolumeInfo.BusySource.VOLUME_CREATE_SEGMENTS);
    try {
      //volume in master
      if (volumeStore.getVolumeForReport(volumeId) != null) {
        String volumeLayout = volumeStore
            .updateVolumeLayoutForReport(volumeId, startSegmentIndex, newSegmentsCount, null);
        volumeStore.updateVolumeLayout(volumeId, startSegmentIndex, newSegmentsCount, volumeLayout);
      } else {
        UpdateVolumeLayoutRequest updateVolumeLayoutRequest = new UpdateVolumeLayoutRequest();
        updateVolumeLayoutRequest.setRequestId(request.getRequestId());
        updateVolumeLayoutRequest.setSegIndex(startSegmentIndex);
        updateVolumeLayoutRequest.setNumToCreate(newSegmentsCount);
        String volumeLayout = volumeInformationManger
            .updateVolumeLayoutToInstance(updateVolumeLayoutRequest);
        volumeStore.updateVolumeLayout(volumeId, startSegmentIndex, newSegmentsCount, volumeLayout);

        if (volumeLayout == null) {
          logger.warn("when createSegment for volume :{}, update the volumeLayout error", volumeId);
          throw new TException();
        }
      }

    } finally {
      lock.setVolumeOperation(LockForSaveVolumeInfo.BusySource.LOCK_WAITING);
      lock.unlock();
    }

    try {
      createVolumeManager
          .createSegments(RequestResponseHelper.buildCreateVolumeRequestFrom(segmentSize, request),
              request.getSegIndex());
    } catch (Exception e) {
      logger.warn("when createSegments for volume :{}, find exception:", request.getVolumeId(), e);
      throw new TException();
    }

    CreateSegmentsResponse response = new CreateSegmentsResponse();
    logger.warn("createSegments response: {}", response);
    return response;
  }

  @Override
  public UpdateVolumeLayoutResponse updateVolumeLayout(UpdateVolumeLayoutRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {

    if (shutDownFlag) {
      throw new ServiceHavingBeenShutdownThrift();
    }

    /* the master notify follower to updateVolumeLayout ***/
    InstanceStatus instanceStatus = appContext.getStatus();
    if (InstanceStatus.SUSPEND != instanceStatus) {
      logger.error("this quest only the master do it, current instance status :{} is not allow",
          instanceStatus);
      throw new ServiceIsNotAvailableThrift().setDetail("I am " + instanceStatus);
    }
    logger.warn("updateVolumeLayout request: {}", request);

    long volumeId = request.getVolumeId();
    VolumeMetadata volumeMetadata = volumeStore.getVolumeForReport(volumeId);
    if (volumeMetadata == null) {
      logger.error("when updateVolumeLayout cannot find the volume:{}", volumeId);
      throw new TException();
    }

    String volumeLayout = volumeStore
        .updateVolumeLayoutForReport(volumeId, request.getSegIndex(), request.getNumToCreate(),
            null);

    UpdateVolumeLayoutResponse response = new UpdateVolumeLayoutResponse();
    response.setRequestId(request.getRequestId());
    response.setVolumeLayout(volumeLayout);

    logger.warn("updateVolumeLayout response: {}", response);
    return response;
  }

  @Override
  public CreateSegmentUnitResponse createSegmentUnit(CreateSegmentUnitRequest request)
      throws NotEnoughSpaceExceptionThrift, SegmentExistingExceptionThrift,
      SegmentUnitBeingDeletedExceptionThrift, NoMemberExceptionThrift,
      ServiceHavingBeenShutdownThrift,
      ServiceIsNotAvailableThrift, TException {

    //check the instance status
    checkInstanceStatus("createSegmentUnit");

    logger.warn("createSegmentUnit request: {}", request);

    try {
      long volumeId = request.getVolumeId();
      request.setSegmentWrapSize(segmentWrappCount);
    } catch (Exception e) {
      logger.error("Caught an exception", e);
      throw new TException(e);
    }

    if (!createSegmentUnitWorkThread.add(request)) {
      String errMsg = "can not create segment unit";
      logger.error(errMsg + ", request: {}", request);
      throw new TException(errMsg);
    }
    CreateSegmentUnitResponse response = new CreateSegmentUnitResponse(request.getRequestId());
    logger.warn("createSegmentUnit response: {}", response);
    return response;
  }

  
  @Override
  public GetVolumeResponse getVolume(GetVolumeRequest request)
      throws VolumeNotFoundExceptionThrift, InvalidInputExceptionThrift,
      ServiceHavingBeenShutdownThrift,
      ServiceIsNotAvailableThrift, AccessDeniedExceptionThrift, AccountNotFoundExceptionThrift {
    if (shutDownFlag) {
      throw new ServiceHavingBeenShutdownThrift();
    }

    // master get volume form follower
    if (InstanceStatus.SUSPEND != appContext.getStatus() && InstanceStatus.HEALTHY != appContext
        .getStatus()) {
      logger.error("Refuse request from remote due to I am not suspend or healthy,"
                      + " request content: {}", request);
      throw new ServiceIsNotAvailableThrift().setDetail("I am not suspend or healthy");
    }

    logger.info("getVolume request: {}", request);

    GetVolumeResponse response = new GetVolumeResponse();
    response.setRequestId(request.getRequestId());

    long volumeId = request.getVolumeId();
    final int startSegmentIndex = request.getStartSegmentIndex();
    final int paginationNumber = request.getPaginationNumber();
    final boolean withSegmentList = !(request.isWithOutSegmentList());
    boolean isEnablePagination = false;

    if (request.isSetEnablePagination()) {
      //if set value, used the value
      isEnablePagination = request.isEnablePagination();
    } else {
      //if not
      isEnablePagination = false;
    }

    VolumeMetadata volumeMetadata = null;
    try {
      volumeMetadata = volumeInformationManger.getVolumeNew(volumeId, request.getAccountId());
    } catch (VolumeNotFoundException e) {
      logger.error("when getVolume, can not find the volume :{}", volumeId);
      throw new VolumeNotFoundExceptionThrift();
    }

    if (volumeMetadata.isMarkDelete()) {
      logger.warn("volume:{} {} has been marked delete, do not return any more",
          volumeMetadata.getVolumeId(),
          volumeMetadata.getName());
      throw new VolumeNotFoundExceptionThrift();
    }

    List<DriverMetadataThrift> driverMetadataThrifts = new ArrayList<>();
    response.setDriverMetadatas(driverMetadataThrifts);

    //check Contain DeadVolume
    boolean notContainDeadVolume = (request.isSetContainDeadVolume() && !request
        .isContainDeadVolume());

    if (notContainDeadVolume) {
      if (volumeMetadata.getVolumeStatus().equals(VolumeStatus.Dead)) {
        // do nothing just return
        logger.warn("do not need to return dead volume:{}, response:{}", volumeMetadata, response);
        return response;
      }
    }

    //for Pagination,to return segment which set the number
    VolumeMetadata volumeMetadataPagination = processVolumeForPagination(volumeMetadata,
        withSegmentList,
        isEnablePagination, startSegmentIndex, paginationNumber);

    VolumeMetadataThrift volumeMetadataThrift = RequestResponseHelper
        .buildThriftVolumeFrom(volumeMetadataPagination, withSegmentList);
    response.setVolumeMetadata(volumeMetadataThrift);

    //just the master can do it
    if (InstanceStatus.HEALTHY == appContext.getStatus()) {
      List<DriverMetadata> volumeBindingDrivers = driverStore.get(volumeId);
      if (volumeBindingDrivers != null && volumeBindingDrivers.size() > 0) {
        for (DriverMetadata driverMetadata : volumeBindingDrivers) {
          response.addToDriverMetadatas(
              RequestResponseHelper.buildThriftDriverMetadataFrom(driverMetadata));
        }
        logger.warn("got drivers:{} with:{}", volumeBindingDrivers, volumeId);
      }
    }

    int segmentCount = withSegmentList ? volumeMetadataThrift.getSegmentsMetadataSwitchSize() : 0;

    if (isEnablePagination) {
      response.setLeftSegment(volumeMetadataPagination.isLeftSegment());
      response.setNextStartSegmentIndex(volumeMetadataPagination.getNextStartSegmentIndex());
    }

    logger.info(
        "getVolume response,isEnablePagination :{} volumeId:{}, with:{} segment count:{}, volume "
            + "status :{} and action:{}, "
            + "isLeftSegment :{}, NextStart index:{} ", isEnablePagination, volumeId,
        withSegmentList,
        segmentCount, volumeMetadataThrift.getVolumeStatus(), volumeMetadataThrift.getInAction(),
        response.isLeftSegment(), response.getNextStartSegmentIndex());

    return response;
  }

  @Override
  public GetSegmentResponse getSegment(GetSegmentRequest request)
      throws InternalErrorThrift, VolumeNotFoundExceptionThrift, ServiceHavingBeenShutdownThrift,
      ServiceIsNotAvailableThrift, TException {

    //check the instance status
    checkInstanceStatus("getSegment");

    logger.warn("getSegment request: {}", request);

    long volumeId = request.getVolumeId();
    VolumeMetadata volumeMetadata = null;

    try {
      volumeMetadata = volumeInformationManger.getVolumeNew(volumeId, SUPERADMIN_ACCOUNT_ID);
    } catch (VolumeNotFoundException e) {
      logger.error("when getSegment, can not find the volume :{}", volumeId);
      throw new VolumeNotFoundExceptionThrift();
    }

    GetSegmentResponse response = new GetSegmentResponse();
    if (volumeMetadata != null) {
      response.setSegment(RequestResponseHelper
          .buildThriftSegmentMetadataFrom(
              volumeMetadata.getSegmentByIndex(request.getSegmentIndex()),
              false));
    }
    response.setRequestId(request.getRequestId());
    response.setStoragePoolId(volumeMetadata.getStoragePoolId());
    logger.warn("getSegment response: {}", response);
    return response;
  }

  @Override
  public GetSegmentSizeResponse getSegmentSize() throws TException {
    return new GetSegmentSizeResponse(segmentSize);
  }

  public void setSegmentSize(long segmentSize) {
    this.segmentSize = segmentSize;
  }

  //when user login loadVolume first
  @Override
  public LoadVolumeResponse loadVolume(LoadVolumeRequest request)
      throws LoadVolumeExceptionThrift {
    logger.debug("loadVolume request {}", request);
    try {
      volumeStore.loadVolumeInDb();
      return new LoadVolumeResponse();
    } catch (Exception e) {
      logger.warn("when loadVolume, find exception:", e);
      throw new LoadVolumeExceptionThrift();
    }
  }

  @Override
  public GetVolumeResponse getVolumeNotDeadByName(GetVolumeRequest request)
      throws InternalErrorThrift, InvalidInputExceptionThrift, NotEnoughSpaceExceptionThrift,
      VolumeNotFoundExceptionThrift, ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      TException {
    //check the instance status
    checkInstanceStatus("getVolumeNotDeadByName");
    logger.warn("getVolumeNotDeadByName request: {}", request);

    VolumeMetadata volumeMetadata = volumeStore.getVolumeNotDeadByName(request.getName());
    if (volumeMetadata == null) {
      logger.warn("Can't find volume metadata given its name {} ", request.getName());
      throw new VolumeNotFoundExceptionThrift();
    }

    long volumeId = volumeMetadata.getVolumeId();
    // iterate all segment unit's and change their statuses accordingly
    GetVolumeResponse response = new GetVolumeResponse();
    response.setVolumeMetadata(RequestResponseHelper.buildThriftVolumeFrom(volumeMetadata, false));

    List<DriverMetadata> volumeBindingDrivers = driverStore.get(volumeId);
    if (volumeBindingDrivers != null && volumeBindingDrivers.size() > 0) {
      for (DriverMetadata driverMetadata : volumeBindingDrivers) {
        if (driverMetadata.getDriverStatus() == DriverStatus.LAUNCHED) {
          response.addToDriverMetadatas(
              RequestResponseHelper.buildThriftDriverMetadataFrom(driverMetadata));
        }
      }
    } else {
      logger.debug("Can't get driver metadata from volume {} ", volumeId);
    }

    response.setRequestId(request.getRequestId());
    logger.warn("getVolumeNotDeadByName response: {}", response);
    return response;
  }

  @Override
  public GetAppliedVolumesResponse getAppliedVolumes(GetAppliedVolumesRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {
    //check the instance status
    checkInstanceStatus("getAppliedVolumes");
    logger.warn("getAppliedVolumes request: {}", request);

    GetAppliedVolumesResponse response = new GetAppliedVolumesResponse();
    response.setRequestId(request.getRequestId());
    response.setVolumeIdList(new ArrayList<>());

    List<VolumeRuleRelationshipInformation> relationshipInfoList = volumeRuleRelationshipStore
        .getByRuleId(request.getRuleId());
    if (relationshipInfoList == null || relationshipInfoList.isEmpty()) {
      return response;
    }

    VolumeMetadata volumeMetadata;
    for (VolumeRuleRelationshipInformation relationshipInfo : relationshipInfoList) {
      volumeMetadata = volumeStore.getVolume(relationshipInfo.getVolumeId());

      if (volumeMetadata == null) {
        logger.warn("volume is not exist,delete it from rule relation ship table,{}",
            relationshipInfo.getVolumeId());
        volumeRuleRelationshipStore.deleteByVolumeId(relationshipInfo.getVolumeId());
        continue;
      }
      response.addToVolumeIdList(relationshipInfo.getVolumeId());
    }

    logger.warn("getAppliedVolumes response: {}", response);
    return response;
  }

  @Override
  public ChangeDriverBoundVolumeResponse changeDriverBoundVolume(
      ChangeDriverBoundVolumeRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {
    //check the instance status
    checkInstanceStatus("changeDriverBoundVolume");
    logger.warn("changeDriverBoundVolume request: {}", request);

    DriverKeyThrift driverKeyThrift = request.getDriver();

    synchronized (driverStore) {
      DriverType driverType = DriverType.valueOf(driverKeyThrift.getDriverType().name());
      DriverMetadata oldDriver = driverStore
          .get(driverKeyThrift.getDriverContainerId(), driverKeyThrift.getVolumeId(), driverType,
              driverKeyThrift.getSnapshotId());
      if (oldDriver != null) {
        oldDriver.setVolumeId(request.getNewVolumeId());
        driverStore.save(oldDriver);
      }
    }

    ChangeDriverBoundVolumeResponse response = new ChangeDriverBoundVolumeResponse();
    response.setRequestId(request.getRequestId());
    logger.warn("changeDriverBoundVolume response: {}", response);
    return response;
  }

  @Override
  public FixVolumeResponseThrift fixVolume(FixVolumeRequestThrift request)
      throws InternalErrorThrift, VolumeNotFoundExceptionThrift, ServiceHavingBeenShutdownThrift,
      ServiceIsNotAvailableThrift, TException {

    checkInstanceStatus("fixVolume");
    logger.warn("fixVolume request: {}", request);

    long volumeId = request.getVolumeId();
    VolumeMetadata volumeMetadata = volumeStore.getVolume(volumeId);
    VolumeInAction volumeInAction = volumeMetadata.getInAction();

    securityManager.hasPermission(request.getAccountId(), "fixVolume");
    securityManager.hasRightToAccess(request.getAccountId(), volumeId);

    boolean needFixVolume = false;
    boolean fixVolumeCompletely = true;

    try {
      volumeMetadata = volumeInformationManger.getVolumeNew(volumeId, request.getAccountId());
    } catch (VolumeNotFoundException e) {
      logger.error("fix volume error,can not found volume:{} and error:", volumeId, e);
      throw new VolumeNotFoundExceptionThrift();
    }

    //check the Operation
    checkExceptionForOperation(volumeMetadata, OperationFunctionType.fixVolume);

    FixVolumeResponseThrift response = new FixVolumeResponseThrift();
    response.setRequestId(request.getRequestId());

    Set<Long> lostDatanodes = new HashSet<>();
    Set<Long> tmpDatanodes = new HashSet<>();

    for (int i = 0; i < volumeMetadata.getSegmentCount(); i++) {
      int needFixCount = 0;
      InstanceId aliveSegmentUnit = null;
      tmpDatanodes.clear();

      SegmentMembership membership = volumeMetadata.getMembership(i);
      if (membership == null) {
        logger.warn("fix volume error:all datanode die,the membership is null {}", request);
        fixVolumeCompletely = false;
        needFixVolume = true;
        continue;
      }
      SegmentMetadata segmentMetadata = volumeMetadata.getSegmentByIndex(i);
      Validate.notNull(membership);
      if (segmentMetadata == null) {
        logger.warn("fix volume error:the segmentMetadata is null,volumeMetadata is: {}",
            volumeMetadata);
        tmpDatanodes.add(membership.getPrimary().getId());
        if (membership.getSecondaries().size() != 0) {
          for (InstanceId entry : membership.getSecondaries()) {
            tmpDatanodes.add(entry.getId());
          }
        }
        if (membership.getJoiningSecondaries().size() != 0) {
          for (InstanceId entry : membership.getJoiningSecondaries()) {
            tmpDatanodes.add(entry.getId());
          }
        }
        if (membership.getArbiters().size() != 0) {
          for (InstanceId entry : membership.getArbiters()) {
            tmpDatanodes.add(entry.getId());
          }
        }

        lostDatanodes.addAll(tmpDatanodes);
        fixVolumeCompletely = false;
        needFixVolume = true;
        continue;
      }

      // judge this segment if need fix
      int memberCount = membership.getMembers().size();
      InstanceId alivePrimaryId = null;
      for (InstanceId instanceId : membership.getMembers()) {
        // datanode down
        InstanceMetadata datanode = storageStore.get(instanceId.getId());
        if (datanode == null || datanode.getDatanodeStatus().equals(UNKNOWN)) {
          logger.warn(" storageStore can not get datanode {}", instanceId);
          needFixCount++;
          tmpDatanodes.add(instanceId.getId());
          continue;
        } else {
          Instance datanodeInstance = instanceStore.get(instanceId);
          Validate.notNull(datanodeInstance);
          if (datanodeInstance.getStatus() != InstanceStatus.HEALTHY) {
            logger.warn(" instanceStore can not get datanode {}", instanceId);
            needFixCount++;
            tmpDatanodes.add(instanceId.getId());
            continue;
          }
        }

        // disk down
        Validate.notNull(datanode);
        SegmentUnitMetadata segmentUnitMetadata = segmentMetadata
            .getSegmentUnitMetadata(instanceId);
        if (segmentUnitMetadata == null) {
          logger.warn("fix volume error:the segmentUnitMetadata is null {}", segmentMetadata);
          needFixCount++;
          continue;
        }
        Validate.notNull(segmentUnitMetadata);
        String diskName = segmentUnitMetadata.getDiskName();
        if (diskName == null || diskName.isEmpty()) {
          logger.warn("fix volume error:the diskName is null {}", segmentUnitMetadata);
        }
        boolean found = false;
        for (RawArchiveMetadata archiveMetadata : datanode.getArchives()) {
          logger.warn("segment unit disk name: {}, archive name: {}", diskName,
              archiveMetadata.getDeviceName());
          if (diskName.equalsIgnoreCase(archiveMetadata.getDeviceName())) {
            found = true;
            if (archiveMetadata.getStatus() != ArchiveStatus.GOOD) {
              logger.warn("fix the volume, archiveMetadata Status is error {}",
                  archiveMetadata.getStatus());
              needFixCount++;
            } else {
              // found and archive is good
              aliveSegmentUnit = instanceId;
              if (membership.getPrimary() == instanceId) {
                alivePrimaryId = instanceId;
              }

            }
            // found, break;
            break;
          }
        }  // for loop to find archive info
        if (!found) {
          logger.warn("fix the volume:archive did not found, diskName is:{}, at datanode:{}",
              diskName,
              datanode.getInstanceId());
          needFixCount++;
        }
      } // for every segment unit in segment

      int aliveCount = memberCount - needFixCount;
      if (aliveCount < volumeMetadata.getVolumeType().getVotingQuorumSize()) {
        needFixVolume = true;
        if (!tmpDatanodes.isEmpty()) {
          lostDatanodes.addAll(tmpDatanodes);
        }
        // can we fix this segment completely
        if (!fixVolumeCompletely) {
          continue;
        }
        if (aliveSegmentUnit == null) {
         
          fixVolumeCompletely = false;
        } else {
          if (membership.isJoiningSecondary(aliveSegmentUnit) || membership
              .isArbiter(aliveSegmentUnit)) {
           
            fixVolumeCompletely = false;
          } else if (membership.isSecondary(aliveSegmentUnit)) {
           
            SegmentUnitMetadata aliveSegmentUnitMetadata = segmentMetadata
                .getSegmentUnitMetadata(aliveSegmentUnit);
            SegmentForm segmentForm = SegmentForm
                .getSegmentForm(aliveSegmentUnitMetadata.getMembership(),
                    volumeMetadata.getVolumeType());
            if (segmentForm == SegmentForm.PSS) {
              fixVolumeCompletely = false;
            }
          }
        }
      } else if (alivePrimaryId == null) {
       
        SegmentUnitMetadata aliveSegmentUnitMetadata = segmentMetadata
            .getSegmentUnitMetadata(aliveSegmentUnit);
        SegmentForm segmentForm = SegmentForm
            .getSegmentForm(aliveSegmentUnitMetadata.getMembership(),
                volumeMetadata.getVolumeType());
        if (!segmentForm.canGenerateNewPrimary()) {
          fixVolumeCompletely = false;
          needFixVolume = true;
          lostDatanodes.add(aliveSegmentUnitMetadata.getMembership().getPrimary().getId());
        }

      }
    }

    response.setLostDatanodes(lostDatanodes);
    response.setFixVolumeCompletely(fixVolumeCompletely);
    response.setNeedFixVolume(needFixVolume);
    logger.warn("fixVolume response: {}", response);
    return response;
  }

  @Override
  public ChangeVolumeStatusFromFixToUnavailableResponse changeVolumeStatusFromFixToUnavailable(
      ChangeVolumeStatusFromFixToUnavailableRequest request)
      throws InternalErrorThrift, VolumeNotFoundExceptionThrift, ServiceHavingBeenShutdownThrift,
      ServiceIsNotAvailableThrift, TException {

    //check the instance status
    checkInstanceStatus("changeVolumeStatusFromFixToUnavailable");
    logger.warn("changeVolumeStatusFromFixToUnavailable request: {}", request);

    ChangeVolumeStatusFromFixToUnavailableResponse changeVolume2
        = new ChangeVolumeStatusFromFixToUnavailableResponse();

    long volumeId = request.getVolumeId();
    VolumeMetadata volumeMetadata;
    try {
      volumeMetadata = volumeInformationManger.getVolumeNew(volumeId, request.getAccountId());
    } catch (VolumeNotFoundException e) {
      logger.error("when changeVolumeStatusFromFixToUnavailable,can not found volume:{}", volumeId,
          e);
      throw new VolumeNotFoundExceptionThrift();
    }

    if (volumeMetadata.getVolumeStatus().equals(VolumeStatus.Fixing)) {
      volumeMetadata.setVolumeStatus(VolumeStatus.Unavailable);
      volumeStore.saveVolume(volumeMetadata);
    }

    changeVolume2.setChangeSucess(true);
    logger.warn("changeVolumeStatusFromFixToUnavailable response: {}",
        changeVolume2);
    return changeVolume2;
  }

  @Override
  public MarkVolumesReadWriteResponse markVolumesReadWrite(MarkVolumesReadWriteRequest request)
      throws ServiceIsNotAvailableThrift, ServiceHavingBeenShutdownThrift,
      PermissionNotGrantExceptionThrift,
      AccountNotFoundExceptionThrift, TException {

    //check the instance status
    checkInstanceStatus("markVolumesReadWrite");
    logger.warn("markVolumesReadWrite request: {}", request);

    if (!request.isSetReadWrite()) {
      logger.warn("when markVolumesReadWrite, not set ReadWrite");
      throw new TException("ReadWrite field should not be null.");
    }

    Long accountId = request.getAccountId();
    //        securityManager.hasPermission(accountId, "markVolumesReadWrite");
    final Set<Long> accessibleResource;
    accessibleResource = securityManager
        .getAccessibleResourcesByType(accountId, PyResource.ResourceType.Volume);
    Set<Long> newVolumesToMark = request.getVolumeIds().stream()
        .filter(l -> accessibleResource.contains(l))
        .collect(Collectors.toSet());

    for (Long volumeId : newVolumesToMark) {

      LockForSaveVolumeInfo.VolumeLockInfo lock = lockForSaveVolumeInfo
          .getVolumeLockInfo(volumeId, LockForSaveVolumeInfo.BusySource.VOLUME_MARKING_READ_WRITE);

      try {
        markVolumeReadWrite(volumeId, request.getReadWrite());
      } finally {
        lock.setVolumeOperation(LockForSaveVolumeInfo.BusySource.LOCK_WAITING);
        lock.unlock();
      }
    }

    MarkVolumesReadWriteResponse response = new MarkVolumesReadWriteResponse(
        request.getRequestId());
    logger.warn("markVolumesReadWrite response: {}", response);
    return response;
  }

  public void markVolumeReadWrite(long volumeId, ReadWriteTypeThrift readWriteTypeThrift)
      throws VolumeNotFoundExceptionThrift {
    VolumeMetadata volume = volumeStore.getVolume(volumeId);
    if (null == volume) {
      logger.warn("when markVolumesReadWrite, can not find the volume :{}", volumeId);
      throw new VolumeNotFoundExceptionThrift();
    }

    VolumeMetadata.ReadWriteType readWriteType = RequestResponseHelper
        .buildReadWriteTypeFrom(readWriteTypeThrift);
    if (readWriteType != volume.getReadWrite()) {
      volume.setReadWrite(readWriteType);
      volumeStore.saveVolume(volume);
    }
  }

  @Override
  public ConfirmFixVolumeResponseThrift confirmFixVolume(ConfirmFixVolumeRequestThrift request)
      throws VolumeNotFoundExceptionThrift, AccessDeniedExceptionThrift,
      LackDatanodeExceptionThrift,
      ServiceIsNotAvailableThrift, InvalidInputExceptionThrift, NotEnoughSpaceExceptionThrift,
      VolumeFixingOperationExceptionThrift, ServiceHavingBeenShutdownThrift,
      FrequentFixVolumeRequestThrift,
      PermissionNotGrantExceptionThrift, AccountNotFoundExceptionThrift, TException {

    //check the instance status
    checkInstanceStatus("confirmFixVolume");
    logger.warn("confirmFixVolume request: {}", request);
    securityManager.hasPermission(request.getAccountId(), "confirmFixVolume");
    securityManager.hasRightToAccess(request.getAccountId(), request.getVolumeId());

    boolean addSuccess = true;
    long volumeId = request.getVolumeId();

    LockForSaveVolumeInfo.VolumeLockInfo lock = lockForSaveVolumeInfo
        .getVolumeLockInfo(volumeId, LockForSaveVolumeInfo.BusySource.VOLUME_FIXTING);

    ConfirmFixVolumeResponseThrift confirmFixVolumeRsp = new ConfirmFixVolumeResponseThrift();
    confirmFixVolumeRsp.setConfirmFixVolumeSucess(false);
    try {
      ConfirmFixVolumeResponse confirmFixVolRsp;
      try {
        logger.warn("client.confirmFixVolume request: {}", request);
        confirmFixVolRsp = confirmFixVolume_tmp(request);

      } finally {
        lock.setVolumeOperation(LockForSaveVolumeInfo.BusySource.LOCK_WAITING);
        lock.unlock();
      }

      logger.warn("confirm fix volume response is {}", confirmFixVolRsp);
      Map<SegIdThrift, List<CreateSegmentUnitInfo>> createSegmentUnitsMap = confirmFixVolRsp
          .getCreateSegmentUnits();

      for (Entry<SegIdThrift, List<CreateSegmentUnitInfo>> entry : createSegmentUnitsMap
          .entrySet()) {
        SegIdThrift segIdThrift = entry.getKey();
        for (CreateSegmentUnitInfo createSegmentUnitInfo : entry.getValue()) {
          CreateSegmentUnitRequest createSegmentUnitRequest = new CreateSegmentUnitRequest();
          createSegmentUnitRequest.setFixVolume(true);
          createSegmentUnitRequest.setVolumeType(confirmFixVolRsp.getVolumeType());
          createSegmentUnitRequest.setStoragePoolId(confirmFixVolRsp.getStoragePoolId());
          createSegmentUnitRequest.setVolumeSource(confirmFixVolRsp.getVolumeSource());
          createSegmentUnitRequest.setRequestId(request.getRequestId());
          createSegmentUnitRequest.setVolumeId(segIdThrift.getVolumeId());
          createSegmentUnitRequest.setSegIndex(segIdThrift.getSegmentIndex());

          createSegmentUnitRequest
              .setSegmentMembershipMap(createSegmentUnitInfo.getSegmentMembershipMap());
          createSegmentUnitRequest.setSegmentRole(createSegmentUnitInfo.getSegmentUnitRole());
          createSegmentUnitRequest.setSegmentWrapSize(segmentWrappCount);

          addSuccess = createSegmentUnitWorkThread.add(createSegmentUnitRequest);
          if (!addSuccess) {
            //add CreateSegmentUnit error
            logger.error("add innerCreateSegmentUnit error, {}", createSegmentUnitRequest);
            ChangeVolumeStatusFromFixToUnavailableRequest changeVolume1
                = new ChangeVolumeStatusFromFixToUnavailableRequest();
            changeVolume1.setRequestId(request.getRequestId());
            changeVolume1.setAccountId(request.getAccountId());
            changeVolume1.setVolumeId(request.getVolumeId());
            changeVolumeStatusFromFixToUnavailable(changeVolume1);
            throw new FrequentFixVolumeRequestThrift();
          }
        }
      }
    } catch (VolumeNotFoundExceptionThrift e) {
      logger.error("caught an exception --", request, e);
      throw e;
    } catch (AccessDeniedExceptionThrift ad) {
      logger.error("caught an exception --AccessDenied", request, ad);
      throw ad;
    } catch (ServiceIsNotAvailableThrift ad) {
      logger.error("caught an exception --ServiceIsNotAvailableThrift", request, ad);
      throw ad;
    } catch (LackDatanodeExceptionThrift ad) {
      logger.error("caught an exception --LackDatanodeExceptionThrift", request, ad);
      throw ad;
    } catch (InvalidInputExceptionThrift ad) {
      logger.error("caught an exception --InvalidInputExceptionThrift", request, ad);
      throw ad;
    } catch (NotEnoughSpaceExceptionThrift ad) {
      logger.error("caught an exception --:NotEnoughSpaceExceptionThrift", request, ad);
      throw ad;
    } catch (VolumeFixingOperationExceptionThrift vfo) {
      logger.error("caught an exception --VolumeFixingOperationExceptionThrift", request, vfo);
      throw vfo;
    } catch (ServiceHavingBeenShutdownThrift shis) {
      logger.error("caught an exception --ServiceHavingBeenShutdownThrift", request, shis);
      throw shis;
    } catch (FrequentFixVolumeRequestThrift ffv) {
      logger.error("caught an exception --FrequentFixVolumeRequestThrift", request, ffv);
      throw ffv;
    } catch (InternalErrorThrift e) {
      logger.error("caught an exception --", request, e);
      throw new TException(e);
    } catch (TException e) {
      logger.error("Caught an exception", e);
      throw filterTexception(e);
    } catch (Exception e) {
      logger.error("caught an exception --", request, e);
      throw new TException(e);
    }
    confirmFixVolumeRsp.setConfirmFixVolumeSucess(addSuccess);
    logger.warn("confirmFixVolume response: {}", confirmFixVolumeRsp);
    return confirmFixVolumeRsp;
  }


  
  public ConfirmFixVolumeResponse confirmFixVolume_tmp(ConfirmFixVolumeRequestThrift request)
      throws InternalErrorThrift, VolumeNotFoundExceptionThrift, LackDatanodeExceptionThrift,
      ServiceIsNotAvailableThrift, InvalidInputExceptionThrift, NotEnoughSpaceExceptionThrift,
      VolumeFixingOperationExceptionThrift, ServiceHavingBeenShutdownThrift, TException {
    logger.warn("confirmFixVolume_tmp request: {}", request);

    ConfirmFixVolumeResponse confirmFixVolumeResponse = new ConfirmFixVolumeResponse();
    confirmFixVolumeResponse.setResponseId(request.getRequestId());
    Set<Long> fixDataNode = request.getLostDatanodes();

    boolean canFix;
    InstanceId lackDataNodeId = null;
    VolumeMetadata volumeMetadata = null;

    try {
      volumeMetadata = volumeInformationManger
          .getVolumeNew(request.getVolumeId(), request.getAccountId());
      Validate.isTrue(volumeMetadata.getVolumeType() == VolumeType.REGULAR
          || volumeMetadata.getVolumeType() == VolumeType.SMALL
          || volumeMetadata.getVolumeType() == VolumeType.LARGE);
    } catch (VolumeNotFoundException e) {
      logger.error("when confirmFixVolume,can not found volume:{}", request.getVolumeId(), e);
      throw new VolumeNotFoundExceptionThrift();
    }

    if (volumeMetadata.getVolumeStatus().equals(VolumeStatus.Fixing)) {
      logger.error("volumeMetadata status is Fixing");
      throw new VolumeFixingOperationExceptionThrift();
    }

    confirmFixVolumeResponse.setVolumeType(volumeMetadata.getVolumeType().getVolumeTypeThrift());
    confirmFixVolumeResponse.setStoragePoolId(volumeMetadata.getStoragePoolId());
    Map<SegIdThrift, List<CreateSegmentUnitInfo>> createSegmentUnitInfoMap = new HashMap<>();

    for (int i = 0; i < volumeMetadata.getSegmentCount(); i++) {
      int needFixCount = 0;
      InstanceId aliveSegmentUnit = null;
      canFix = true;
      List<CreateSegmentUnitInfo> tmpSegmentUnitInfoList = new ArrayList<>();
      SegmentMembership membership = volumeMetadata.getMembership(i);
      SegmentVersion segmentVersion;

      Set<Long> exceptNodeId = new HashSet<>();
      SegId segId;
      int aliveSegmentUnitCount;
      InstanceId alivePrimaryId = null;
      SegmentMetadata segmentMetadata = null;
      if (membership == null) {
        logger.warn("fix volume error: all datanodes are dead, the membership is null {}", request);
        aliveSegmentUnitCount = 0;
        canFix = true;
        segmentVersion = new SegmentVersion(1, 0);
        segId = volumeMetadata.getActualSegIdBySegIndex(i);
      } else {
        final int memberCount = membership.getMembers().size();
        segmentVersion = membership.getSegmentVersion();
        segmentVersion = segmentVersion.incEpoch();
        Validate.notEmpty(membership.getMembers());
        segmentMetadata = volumeMetadata.getSegmentByIndex(i);

        // judge this segment if need fix
        segId = segmentMetadata.getSegId();
        for (InstanceId instanceId : membership.getMembers()) {
          // datanode down
          InstanceMetadata datanode = storageStore.get(instanceId.getId());
          if (datanode == null) {
            if (fixDataNode == null || !fixDataNode.contains(instanceId.getId())) {
              logger.error("can not fix, need datanode {}", instanceId);
              canFix = false;
              lackDataNodeId = instanceId;
            }
            needFixCount++;
            continue;
          } else {
            Instance datanodeInstance = instanceStore.get(instanceId);
            Validate.notNull(datanodeInstance);
            if (datanodeInstance.getStatus() != InstanceStatus.HEALTHY) {
              needFixCount++;
              if (fixDataNode == null || !fixDataNode.contains(instanceId.getId())) {
                logger.error("can not fix, need datanode {}", instanceId);
                canFix = false;
                lackDataNodeId = instanceId;
              }
              continue;
            }
          }
          // disk down
          Validate.notNull(datanode);
          SegmentUnitMetadata segmentUnitMetadata = segmentMetadata
              .getSegmentUnitMetadata(instanceId);
          if (segmentUnitMetadata == null) {
            logger.error("fix volume error: the segmentUnitMetadata is null {}", segmentMetadata);
            needFixCount++;
            continue;
          }
          Validate.notNull(segmentUnitMetadata);
          String diskName = segmentUnitMetadata.getDiskName();
          if (diskName == null || diskName.isEmpty()) {
            logger.error("fix volume error: the diskName is null {}", segmentUnitMetadata);
          }
          boolean found = false;
          for (RawArchiveMetadata archiveMetadata : datanode.getArchives()) {
            logger.info("segment unit disk name: {}, archive name: {}", diskName,
                archiveMetadata.getDeviceName());
            if (diskName.equalsIgnoreCase(archiveMetadata.getDeviceName())) {
              found = true;
              if (archiveMetadata.getStatus() != ArchiveStatus.GOOD) {
                needFixCount++;
                break;
              } else {
                // found and archive is good
                exceptNodeId.add(instanceId.getId());
                aliveSegmentUnit = instanceId;
                if (membership.getPrimary() == instanceId) {
                  alivePrimaryId = instanceId;
                }
              }
            }
          }  // for loop to find archive info
          if (!found) {
            needFixCount++;
          }
        } // for every segment unit in segment
        aliveSegmentUnitCount = memberCount - needFixCount;
      }
      //wo need fix volume
      ReserveSegUnitResult reserveSegUnitResult;
      if (aliveSegmentUnitCount < volumeMetadata.getVolumeType().getVotingQuorumSize()) {
        logger.warn("we need fix volume,alive segment count,{}", aliveSegmentUnitCount);
        // can we fix this segment completely
        if (!canFix) {
          logger.error(" confirmFixVolume throw error ,lack DataNode ID is {}", lackDataNodeId);
          throw new LackDatanodeExceptionThrift();
        }
        if (exceptNodeId.isEmpty()) {
          //no segmentUnit alive in segment,need create two segmentUnit
          Validate.isTrue(aliveSegmentUnitCount == 0);
          CreateSegmentUnitInfo tmpPrimarySegmentUnitInfoDetail = new CreateSegmentUnitInfo();
          CreateSegmentUnitInfo tmpSecondarySegmentUnitInfoDetail = new CreateSegmentUnitInfo();
          CreateSegmentUnitInfo tmpArbiterSegmentUnitInfoDetail = new CreateSegmentUnitInfo();

          Map<InstanceIdAndEndPointThrift, SegmentMembershipThrift> primary1 = new HashMap<>();

          Set<InstanceId> secondaryId = new HashSet<>();
          Set<InstanceId> joiningSecondaryId = new HashSet<>();
          Set<InstanceId> arbiterId = new HashSet<>();

          //reserve unit ,get datanode

          ReserveSegUnitsInfo reserveSegUnitsInfo = new ReserveSegUnitsInfo(
              volumeMetadata.getSegmentSize(),
              exceptNodeId, 2, request.getVolumeId(), segId.getIndex(),
              convertFromSegmentUnitTypeThrift(SegmentUnitTypeThrift.Normal));

          try {
            reserveSegUnitResult = segmentUnitsDistributionManager
                .reserveSegUnits(reserveSegUnitsInfo);
          } catch (NotEnoughSpaceExceptionThrift ne) {
            logger.error("confirmFixVolume throw error:  NotEnoughSpaceExceptionThrift");
            throw new NotEnoughSpaceExceptionThrift();

          }

          List<InstanceMetadataThrift> firstList = new ArrayList<>();
          List<InstanceMetadataThrift> secondList = new ArrayList<>();
          splitDatanodesByGroupId(reserveSegUnitResult, firstList, secondList);

          Validate.notEmpty(firstList);
          Validate.notEmpty(secondList);
          InstanceIdAndEndPointThrift firstInstanceIdAndEndPointThrift = RequestResponseHelper
              .buildInstanceIdAndEndPointThriftList(firstList).get(0);
          InstanceIdAndEndPointThrift secondInstanceIdAndEndPointThrift = RequestResponseHelper
              .buildInstanceIdAndEndPointThriftList(secondList).get(0);

          if (volumeMetadata.getVolumeType() == VolumeType.REGULAR) {
            logger.warn("all segmentUnit died ,we need create P+S");
            //create membership
            InstanceId primaryId = new InstanceId(
                firstInstanceIdAndEndPointThrift.getInstanceId());
            InstanceId secondaryInstanceId = new InstanceId(
                secondInstanceIdAndEndPointThrift.getInstanceId());
            secondaryId.add(secondaryInstanceId);
            SegmentMembership tmpMembership = new SegmentMembership(segmentVersion, primaryId,
                secondaryId,
                arbiterId, null, joiningSecondaryId);

            SegmentMembershipThrift tmpMemberShipThrift = RequestResponseHelper
                .buildThriftMembershipFrom(segId, tmpMembership);
           
            tmpPrimarySegmentUnitInfoDetail.setSegmentUnitRole(SegmentUnitRoleThrift.Primary);
            primary1
                .put(firstInstanceIdAndEndPointThrift, tmpMemberShipThrift);
            tmpPrimarySegmentUnitInfoDetail
                .setSegmentMembershipMap(primary1);

            tmpSegmentUnitInfoList.add(tmpPrimarySegmentUnitInfoDetail);

           
            Map<InstanceIdAndEndPointThrift, SegmentMembershipThrift> sendary1 = new HashMap<>();
            tmpSecondarySegmentUnitInfoDetail.setSegmentUnitRole(SegmentUnitRoleThrift.Secondary);
            sendary1
                .put(secondInstanceIdAndEndPointThrift, tmpMemberShipThrift);
            tmpSecondarySegmentUnitInfoDetail
                .setSegmentMembershipMap(sendary1);
            tmpSegmentUnitInfoList.add(tmpSecondarySegmentUnitInfoDetail);
          } else if (volumeMetadata.getVolumeType() == VolumeType.SMALL) {
            logger.warn("all segmentUnit died ,we need create P+A");
           
            InstanceId primaryId = new InstanceId(
                firstInstanceIdAndEndPointThrift.getInstanceId());
            InstanceId arbiterinstanceId = new InstanceId(
                secondInstanceIdAndEndPointThrift.getInstanceId());
            arbiterId.add(arbiterinstanceId);
            SegmentMembership tmpMembership = new SegmentMembership(segmentVersion, primaryId,
                secondaryId,
                arbiterId, null, joiningSecondaryId);
            SegmentMembershipThrift tmpMemberShipThrift = RequestResponseHelper
                .buildThriftMembershipFrom(segId, tmpMembership);

            //create segment unit as P
            tmpPrimarySegmentUnitInfoDetail.setSegmentUnitRole(SegmentUnitRoleThrift.Primary);
            primary1
                .put(firstInstanceIdAndEndPointThrift, tmpMemberShipThrift);
            tmpPrimarySegmentUnitInfoDetail
                .setSegmentMembershipMap(primary1);
            tmpSegmentUnitInfoList.add(tmpPrimarySegmentUnitInfoDetail);

            // create segment unit as A
            Map<InstanceIdAndEndPointThrift, SegmentMembershipThrift> arbiter1 = new HashMap<>();
            tmpArbiterSegmentUnitInfoDetail.setSegmentUnitRole(SegmentUnitRoleThrift.Arbiter);
            arbiter1
                .put(secondInstanceIdAndEndPointThrift, tmpMemberShipThrift);
            tmpArbiterSegmentUnitInfoDetail
                .setSegmentMembershipMap(arbiter1);
            tmpSegmentUnitInfoList.add(tmpArbiterSegmentUnitInfoDetail);
          }
        } else {
          //  only one segmentUnit alive in segment,need create another segment unit to vote
          ReserveSegUnitsInfo reserveSegUnitsInfo = new ReserveSegUnitsInfo(
              volumeMetadata.getSegmentSize(),
              exceptNodeId, 1, request.getVolumeId(), segId.getIndex(),
              convertFromSegmentUnitTypeThrift(SegmentUnitTypeThrift.Normal));

          try {
            reserveSegUnitResult = segmentUnitsDistributionManager
                .reserveSegUnits(reserveSegUnitsInfo);
          } catch (NotEnoughSpaceExceptionThrift ne) {
            logger.error("confirmFixVolume throw error:  NotEnoughSpaceExceptionThrift");
            throw new NotEnoughSpaceExceptionThrift();
          }

          CreateSegmentUnitInfo tmpSegmentUnitInfoDetail = new CreateSegmentUnitInfo();
          Map<InstanceIdAndEndPointThrift, SegmentMembershipThrift> segmentMembershipThriftMap =
              new HashMap<>();
          tmpSegmentUnitInfoList.add(tmpSegmentUnitInfoDetail);

          Set<InstanceId> secondaryId = membership.getSecondaries();
          Set<InstanceId> joiningSecondaryId = membership.getJoiningSecondaries();
          Set<InstanceId> arbiterId = membership.getArbiters();

          if (volumeMetadata.getVolumeType() == VolumeType.REGULAR) {
            if (membership.isJoiningSecondary(aliveSegmentUnit)) {
             
              logger.warn("segmentUnit only J alive, volume type is PSS ,we need create P");
              secondaryId.clear();
              arbiterId.clear();
              tmpSegmentUnitInfoDetail.setSegmentUnitRole(SegmentUnitRoleThrift.Primary);
              for (InstanceIdAndEndPointThrift instanceIdAndEndPointThrift : RequestResponseHelper
                  .buildInstanceIdAndEndPointThriftList(reserveSegUnitResult.getInstances())) {
                InstanceId primaryId = new InstanceId(instanceIdAndEndPointThrift.getInstanceId());
                SegmentMembership tmpMembership = new SegmentMembership(segmentVersion, primaryId,
                    secondaryId, arbiterId, null, joiningSecondaryId);
                SegmentMembershipThrift tmpMemberShipThrift = RequestResponseHelper
                    .buildThriftMembershipFrom(segId, tmpMembership);
                segmentMembershipThriftMap.put(instanceIdAndEndPointThrift, tmpMemberShipThrift);
              }
            } else if (membership.isSecondary(aliveSegmentUnit)) {
             
              logger.warn("segmentUnit only S alive, volume type is PSS ,we need create J");
              tmpSegmentUnitInfoDetail.setSegmentUnitRole(SegmentUnitRoleThrift.JoiningSecondary);
              for (InstanceIdAndEndPointThrift instanceIdAndEndPointThrift : RequestResponseHelper
                  .buildInstanceIdAndEndPointThriftList(reserveSegUnitResult.getInstances())) {
               
                secondaryId.clear();
                arbiterId.clear();
                InstanceId insertInstanceId = new InstanceId(
                    instanceIdAndEndPointThrift.getInstanceId());
                joiningSecondaryId.clear();
                joiningSecondaryId.add(insertInstanceId);
                SegmentMembership tmpMembership = new SegmentMembership(segmentVersion,
                    aliveSegmentUnit, secondaryId, arbiterId, null, joiningSecondaryId);
                SegmentMembershipThrift tmpMemberShipThrift = RequestResponseHelper
                    .buildThriftMembershipFrom(segId, tmpMembership);
                segmentMembershipThriftMap.put(instanceIdAndEndPointThrift, tmpMemberShipThrift);
              }
            } else if (membership.isPrimary(aliveSegmentUnit)) {
             
              logger.warn("segmentUnit only P alive ,volume type is PSS ,we need create J");
              for (InstanceIdAndEndPointThrift instanceIdAndEndPointThrift : RequestResponseHelper
                  .buildInstanceIdAndEndPointThriftList(reserveSegUnitResult.getInstances())) {

                secondaryId.clear();
                arbiterId.clear();
                InstanceId insertInstanceId = new InstanceId(
                    instanceIdAndEndPointThrift.getInstanceId());
                joiningSecondaryId.clear();
                joiningSecondaryId.add(insertInstanceId);
                SegmentMembership tmpMembership = new SegmentMembership(segmentVersion,
                    membership.getPrimary(), secondaryId, arbiterId, null, joiningSecondaryId);
                SegmentMembershipThrift tmpMemberShipThrift = RequestResponseHelper
                    .buildThriftMembershipFrom(segId, tmpMembership);
                segmentMembershipThriftMap.put(instanceIdAndEndPointThrift, tmpMemberShipThrift);
              }
              tmpSegmentUnitInfoDetail.setSegmentUnitRole(SegmentUnitRoleThrift.JoiningSecondary);
            } else {
              logger.error("PSS Error,aliveSegmentUnit is not in membership");
            }
          } else if (volumeMetadata.getVolumeType() == VolumeType.SMALL) {
            if (membership.isJoiningSecondary(aliveSegmentUnit)) {
             
              logger.warn("segmentUnit only J alive ,volume type is PSA ,we need create P");
              tmpSegmentUnitInfoDetail.setSegmentUnitRole(SegmentUnitRoleThrift.Primary);
              for (InstanceIdAndEndPointThrift instanceIdAndEndPointThrift : RequestResponseHelper
                  .buildInstanceIdAndEndPointThriftList(reserveSegUnitResult.getInstances())) {
                secondaryId.clear();
                arbiterId.clear();
                joiningSecondaryId.clear();
                joiningSecondaryId.add(aliveSegmentUnit);
                InstanceId insertInstanceId = new InstanceId(
                    instanceIdAndEndPointThrift.getInstanceId());
                SegmentMembership tmpMembership = new SegmentMembership(segmentVersion,
                    insertInstanceId, secondaryId, arbiterId, null, joiningSecondaryId);
                SegmentMembershipThrift tmpMemberShipThrift = RequestResponseHelper
                    .buildThriftMembershipFrom(segId, tmpMembership);
                segmentMembershipThriftMap.put(instanceIdAndEndPointThrift, tmpMemberShipThrift);
              }
            } else if (membership.isArbiter(aliveSegmentUnit)) {
             
              logger.warn("segmentUnit only A alive ,volume type is PSA ,we need create P");
              tmpSegmentUnitInfoDetail.setSegmentUnitRole(SegmentUnitRoleThrift.Primary);

              for (InstanceIdAndEndPointThrift instanceIdAndEndPointThrift : RequestResponseHelper
                  .buildInstanceIdAndEndPointThriftList(reserveSegUnitResult.getInstances())) {
                secondaryId.clear();
                joiningSecondaryId.clear();
                InstanceId insertInstanceId = new InstanceId(
                    instanceIdAndEndPointThrift.getInstanceId());

                SegmentMembership tmpMembership = new SegmentMembership(segmentVersion,
                    insertInstanceId, secondaryId, arbiterId, null, joiningSecondaryId);
                SegmentMembershipThrift tmpMemberShipThrift = RequestResponseHelper
                    .buildThriftMembershipFrom(segId, tmpMembership);
                segmentMembershipThriftMap.put(instanceIdAndEndPointThrift, tmpMemberShipThrift);
              }
            } else if (membership.isSecondary(aliveSegmentUnit)) {
             
              logger.warn("segmentUnit only S alive ,volume type is PSA ,we need create A");
              tmpSegmentUnitInfoDetail.setSegmentUnitRole(SegmentUnitRoleThrift.Arbiter);

              for (InstanceIdAndEndPointThrift instanceIdAndEndPointThrift : RequestResponseHelper
                  .buildInstanceIdAndEndPointThriftList(reserveSegUnitResult.getInstances())) {
                InstanceId insertInstanceId = new InstanceId(
                    instanceIdAndEndPointThrift.getInstanceId());
                secondaryId.clear();
                arbiterId.clear();
                arbiterId.add(insertInstanceId);
                joiningSecondaryId.clear();
                SegmentMembership tmpMembership = new SegmentMembership(segmentVersion,
                    aliveSegmentUnit, secondaryId, arbiterId, null, joiningSecondaryId);
                SegmentMembershipThrift tmpMemberShipThrift = RequestResponseHelper
                    .buildThriftMembershipFrom(segId, tmpMembership);
                segmentMembershipThriftMap.put(instanceIdAndEndPointThrift, tmpMemberShipThrift);
              }
            } else if (membership.isPrimary(aliveSegmentUnit)) {
              // P alive,need create Arbiter;
              logger.warn("segmentUnit only P alive ,volume type is PSA ,we need create A");
              tmpSegmentUnitInfoDetail.setSegmentUnitRole(SegmentUnitRoleThrift.Arbiter);

              for (InstanceIdAndEndPointThrift instanceIdAndEndPointThrift : RequestResponseHelper
                  .buildInstanceIdAndEndPointThriftList(reserveSegUnitResult.getInstances())) {
                InstanceId insertInstanceId = new InstanceId(
                    instanceIdAndEndPointThrift.getInstanceId());
                arbiterId.clear();
                arbiterId.add(insertInstanceId);
                secondaryId.clear();
                joiningSecondaryId.clear();
                SegmentMembership tmpMembership = new SegmentMembership(segmentVersion,
                    aliveSegmentUnit, secondaryId, arbiterId, null, joiningSecondaryId);
                SegmentMembershipThrift tmpMemberShipThrift = RequestResponseHelper
                    .buildThriftMembershipFrom(segId, tmpMembership);
                segmentMembershipThriftMap.put(instanceIdAndEndPointThrift, tmpMemberShipThrift);
              }
            } else {
              logger.error("PSA Error,aliveSegmentUnit is not in membership");
            }
          }

          tmpSegmentUnitInfoDetail.setSegmentMembershipMap(segmentMembershipThriftMap);
        }

        if (!tmpSegmentUnitInfoList.isEmpty()) {
          createSegmentUnitInfoMap
              .put(new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex()),
                  tmpSegmentUnitInfoList);
        }
      } else if (alivePrimaryId == null) {
        // P is not alive

        logger.warn("we need fix volume,alive segment count,{}", aliveSegmentUnitCount);
        // can we fix this segment completely
        if (!canFix) {
          logger.error(" confirmFixVolume throw error ,lack DataNode ID is {}", lackDataNodeId);
          throw new LackDatanodeExceptionThrift();
        }

        Validate.notNull(segmentMetadata);
        SegmentUnitMetadata aliveSegmentUnitMetadata = segmentMetadata
            .getSegmentUnitMetadata(aliveSegmentUnit);
        SegmentForm segmentForm = SegmentForm
            .getSegmentForm(aliveSegmentUnitMetadata.getMembership(),
                volumeMetadata.getVolumeType());
        //PJA and P is not Alive
        if (!segmentForm.canGenerateNewPrimary()) {
          ReserveSegUnitsInfo reserveSegUnitsInfo = new ReserveSegUnitsInfo(
              volumeMetadata.getSegmentSize(),
              exceptNodeId, 1, request.getVolumeId(), segId.getIndex(),
              convertFromSegmentUnitTypeThrift(SegmentUnitTypeThrift.Normal));

          try {
            reserveSegUnitResult = segmentUnitsDistributionManager
                .reserveSegUnits(reserveSegUnitsInfo);
          } catch (NotEnoughSpaceExceptionThrift ne) {
            logger.error("confirmFixVolume throw error:  NotEnoughSpaceExceptionThrift");
            throw new NotEnoughSpaceExceptionThrift();

          }

          CreateSegmentUnitInfo tmpSegmentUnitInfoDetail = new CreateSegmentUnitInfo();
          Map<InstanceIdAndEndPointThrift, SegmentMembershipThrift> segmentMembershipThriftMap =
              new HashMap<>();
          tmpSegmentUnitInfoList.add(tmpSegmentUnitInfoDetail);

          Set<InstanceId> secondaryId = membership.getSecondaries();
          Set<InstanceId> joiningSecondaryId = membership.getJoiningSecondaries();
          Set<InstanceId> arbiterId = membership.getArbiters();

          tmpSegmentUnitInfoDetail.setSegmentUnitRole(SegmentUnitRoleThrift.Primary);
          for (InstanceIdAndEndPointThrift instanceIdAndEndPointThrift : RequestResponseHelper
              .buildInstanceIdAndEndPointThriftList(reserveSegUnitResult.getInstances())) {
            secondaryId.clear();
            InstanceId insertInstanceId = new InstanceId(
                instanceIdAndEndPointThrift.getInstanceId());
            SegmentMembership tmpMembership = new SegmentMembership(segmentVersion,
                insertInstanceId,
                secondaryId, arbiterId, null, joiningSecondaryId);
            SegmentMembershipThrift tmpMemberShipThrift = RequestResponseHelper
                .buildThriftMembershipFrom(segId, tmpMembership);
            segmentMembershipThriftMap.put(instanceIdAndEndPointThrift, tmpMemberShipThrift);
          }
          tmpSegmentUnitInfoDetail.setSegmentMembershipMap(segmentMembershipThriftMap);
          if (!tmpSegmentUnitInfoList.isEmpty()) {
            createSegmentUnitInfoMap
                .put(new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex()),
                    tmpSegmentUnitInfoList);
          }
        }

      } // if judge this segment needs fix or not
    } // for every segment in volume
    confirmFixVolumeResponse.setCreateSegmentUnits(createSegmentUnitInfoMap);

    confirmFixVolumeResponse
        .setVolumeSource(volumeMetadata.getVolumeSource().getVolumeSourceThrift());

    volumeMetadata.setVolumeStatus(VolumeStatus.Fixing);
    volumeMetadata.markLastFixVolumeTime();
    volumeStore.saveVolume(volumeMetadata);
    logger.warn("confirmFixVolume response: {}", confirmFixVolumeResponse);
    return confirmFixVolumeResponse;
  }


  /*    Volume end  **********/
  @Override
  public CheckVolumeIsReadOnlyResponse checkVolumeIsReadOnly(CheckVolumeIsReadOnlyRequest request)
      throws ServiceIsNotAvailableThrift, ServiceHavingBeenShutdownThrift,
      VolumeIsMarkWriteExceptionThrift,
      VolumeIsAppliedWriteAccessRuleExceptionThrift,
      VolumeIsConnectedByWritePermissionClientExceptionThrift,
      PermissionNotGrantExceptionThrift, TException {

    //check the instance status
    checkInstanceStatus("checkVolumeIsReadOnly");
    logger.warn("checkVolumeIsReadOnly request: {}", request);

    Long volumeId = request.getVolumeId();
    VolumeMetadataAndDrivers volumeMetadataAndDrivers = null;
    VolumeMetadata volume = null;
    List<DriverMetadata> driverMetadataList = null;
    try {
      volumeMetadataAndDrivers = volumeInformationManger
          .getDriverContainVolumes(volumeId, Constants.SUPERADMIN_ACCOUNT_ID, false);
      volume = volumeMetadataAndDrivers.getVolumeMetadata();

      // if until here there is no exception thrown, can get all drivers bound with volume
      driverMetadataList = volumeMetadataAndDrivers.getDriverMetadatas();
    } catch (VolumeNotFoundException e) {
      logger.error("when checkVolumeIsReadOnly, can not find the volume:{}", volumeId);
    }

    LockForSaveVolumeInfo.VolumeLockInfo lock = lockForSaveVolumeInfo
        .getVolumeLockInfo(volumeId, LockForSaveVolumeInfo.BusySource.VOLUME_MARKING_READ_WRITE);

    try {
      if (VolumeMetadata.ReadWriteType.READONLY != volume.getReadWrite()) {
        VolumeIsMarkWriteExceptionThrift volumeIsMarkWriteExceptionThrift =
            new VolumeIsMarkWriteExceptionThrift();
        volumeIsMarkWriteExceptionThrift
            .setDetail("Volume has been marked as " + volume.getReadWrite().name() + ".");
        logger.warn("{}", volumeIsMarkWriteExceptionThrift.getDetail());
        throw volumeIsMarkWriteExceptionThrift;
      }

      // then check whether volume is applied access rule with write permission
      List<VolumeRuleRelationshipInformation> relationshipInfoList = volumeRuleRelationshipStore
          .getByVolumeId(volumeId);
      if (null != relationshipInfoList && !relationshipInfoList.isEmpty()) {
        String exceptionDetailString = "";
        boolean needThrowException = false;
        for (VolumeRuleRelationshipInformation relationshipInfo : relationshipInfoList) {
          AccessRuleInformation accessRuleInformation = accessRuleStore
              .get(relationshipInfo.getRuleId());
          if (AccessPermissionType.READ != AccessPermissionType
              .findByValue(accessRuleInformation.getPermission())) {
            needThrowException = true;
            exceptionDetailString =
                exceptionDetailString + "IP:" + accessRuleInformation.getIpAddress()
                    + ", Permission:"
                    + AccessPermissionType.findByValue(accessRuleInformation.getPermission()).name()
                    + "\n";
          }
        }
        if (needThrowException) {
          VolumeIsAppliedWriteAccessRuleExceptionThrift volume2
              = new VolumeIsAppliedWriteAccessRuleExceptionThrift();
          volume2.setDetail(
              "Volume is applied by access rule with write permission.\n" + exceptionDetailString);
          logger.warn("{}", volume2.getDetail());
          throw volume2;
        }
      }

    } finally {
      lock.setVolumeOperation(LockForSaveVolumeInfo.BusySource.LOCK_WAITING);
      lock.unlock();
    }

    try {
      Set<EndPoint> boundCoordinatorEps = new HashSet<>();
      for (DriverMetadata driver : driverMetadataList) {
        boundCoordinatorEps.add(new EndPoint(driver.getHostName(), driver.getCoordinatorPort()));
      }
      logger.warn("boundCoordinatorEps:{}", boundCoordinatorEps);
      Set<Instance> coordinators = instanceStore
          .getAll(PyService.COORDINATOR.getServiceName(), InstanceStatus.HEALTHY);
      boolean needThrowException = false;
      String exceptionDetail = "";
      for (Instance coordinator : coordinators) {
        EndPoint ioEndpoint = coordinator.getEndPointByServiceName(PortType.IO);
        logger.warn("get coordinator endPoint:{}", ioEndpoint);
        if (!boundCoordinatorEps.contains(ioEndpoint)) {
          logger.warn("didn't contain coordinator:{}", ioEndpoint);
          continue;
        }

        EndPoint endpoint = coordinator.getEndPoint();
        try {
          CoordinatorClientFactory.CoordinatorClientWrapper coordinatorClientWrapper =
              coordinatorClientFactory
                  .build(endpoint);
          GetConnectClientInfoResponse clientInfoResponse = coordinatorClientWrapper.getClient()
              .getConnectClientInfo(
                  new GetConnectClientInfoRequest(RequestIdBuilder.get(), volumeId));
          for (Entry<String, AccessPermissionTypeThrift> entry : clientInfoResponse
              .getConnectClientAndAccessType().entrySet()) {
            if (AccessPermissionTypeThrift.READ != entry.getValue()) {
              needThrowException = true;
              exceptionDetail =
                  exceptionDetail + "Client: " + entry.getKey() + ", Permission: " + entry
                      .getValue()
                      .name() + "\n";
            }
          }
        } catch (GenericThriftClientFactoryException e) {
          logger.error("Caught an exception when build coordinator client {}", endpoint, e);
          throw new NetworkErrorExceptionThrift().setDetail(e.getMessage());
        }
      }

      if (needThrowException) {
        VolumeIsConnectedByWritePermissionClientExceptionThrift volume1 =
            new VolumeIsConnectedByWritePermissionClientExceptionThrift();
        volume1
            .setDetail("Volume is connected by clients with write permission.\n" + exceptionDetail);
        throw volume1;
      }

      CheckVolumeIsReadOnlyResponse checkVolumeIsReadOnlyResponse =
          new CheckVolumeIsReadOnlyResponse();
      checkVolumeIsReadOnlyResponse.setRequestId(request.getRequestId());
      checkVolumeIsReadOnlyResponse.setVolumeId(request.getVolumeId());
      checkVolumeIsReadOnlyResponse.setReadOnly(true);
      logger.warn("checkVolumeIsReadOnly response: {}", checkVolumeIsReadOnlyResponse);
      return checkVolumeIsReadOnlyResponse;
    } catch (VolumeIsConnectedByWritePermissionClientExceptionThrift e) {
      logger.warn(e.getDetail());
      throw e;
    } catch (TException e) {
      logger.error("Caught an exception", e);
      throw filterTexception(e);
    } catch (Exception e) {
      logger.error("caught an exception --", request, e);
      throw new TException(e);
    }
  }
  
  
  public LaunchDriverResponseThrift launchDriver(LaunchDriverRequestThrift request)
      throws AccessDeniedExceptionThrift, VolumeNotFoundExceptionThrift,
      VolumeNotAvailableExceptionThrift,
      NotRootVolumeExceptionThrift, VolumeBeingDeletedExceptionThrift,
      TooManyDriversExceptionThrift,
      ServiceHavingBeenShutdownThrift, DriverTypeConflictExceptionThrift,
      InvalidInputExceptionThrift,
      VolumeWasRollbackingExceptionThrift,
      SnapshotRollingBackExceptionThrift, DriverLaunchingExceptionThrift,
      DriverUnmountingExceptionThrift,
      VolumeDeletingExceptionThrift, SystemMemoryIsNotEnoughThrift, SystemCpuIsNotEnoughThrift,
      DriverAmountAndHostNotFitThrift, DriverHostCannotUseThrift, DriverIsUpgradingExceptionThrift,
      PermissionNotGrantExceptionThrift, AccountNotFoundExceptionThrift,
      DriverTypeIsConflictExceptionThrift,
      ExistsDriverExceptionThrift, DriverNameExistsExceptionThrift,
      VolumeLaunchMultiDriversExceptionThrift,
      ServiceIsNotAvailableThrift, VolumeCanNotLaunchMultiDriversThisTimeExceptionThrift,
      TException {

    //check the instance statusTwoLevelVolumeStoreImpl.java
    checkInstanceStatus("launchDriver");
    logger.warn("launchDriver request: {}", request);

    String volumeName = null;
    int csiCount = 1;
    List<EndPoint> endPoints = new ArrayList<>();
    long volumeId = request.getVolumeId();

    securityManager.hasPermission(request.getAccountId(), "launchDriver");
    securityManager.hasRightToAccess(request.getAccountId(), volumeId);

    VolumeMetadata volumeMetadata = volumeStore.getVolume(volumeId);

    //check the Operation
    checkExceptionForOperation(volumeMetadata, OperationFunctionType.launchDriver);

    LockForSaveVolumeInfo.VolumeLockInfo lock = null;
    lock = lockForSaveVolumeInfo
        .getVolumeLockInfo(volumeId, LockForSaveVolumeInfo.BusySource.DRIVER_LAUNCHING);
    logger.warn("for volume :{}, get the launch lock", volumeId);

    // this section used for transition from NBD to PYD
    boolean isException = false;
    String driverName;
    DriverTypeThrift driverThrift = request.getDriverType();
    if (driverThrift == DriverTypeThrift.NBD) {
      driverName = "PYD";
    } else {
      driverName = driverThrift.name();
    }

    Set<String> driverNameLaunchThisTime = new HashSet<>();
    snapshotLock.writeLock().lock();
    try {
      VolumeMetadata volume;
      List<DriverMetadata> existingDriverList;
      try {
        VolumeMetadataAndDrivers volumeAndDriverInfo = volumeInformationManger
            .getDriverContainVolumes(request.getVolumeId(), request.getAccountId(), true);

        volume = volumeAndDriverInfo.getVolumeMetadata();
        volumeName = volume.getName();
        existingDriverList = volumeAndDriverInfo.getDriverMetadatas();
        logger.warn(
            "when launchDriver, for volume :{}, the existingDriverList :{} and "
                + "size :{}",
            volumeId, existingDriverList, existingDriverList.size());
      } catch (VolumeNotFoundException e) {
        logger.error("when launchDriver, can not find the volume :{}", request.getVolumeId());
        throw new VolumeNotFoundExceptionThrift();
      }

      if (volume.isDeletedByUser()) {
        throw new VolumeBeingDeletedExceptionThrift();
      }

      if (!volume.isVolumeAvailable()) {
        logger.error("Status of volume {} is {}, launchDriver fail!", volume,
            volume.getVolumeStatus());
        throw new VolumeNotAvailableExceptionThrift();
      }

      if (request.isVolumeCanNotLaunchMultiDriversThisTime() && existingDriverList != null
          && !existingDriverList.isEmpty()) {
        logger.warn(
            "Volume is not allowed to launch more than one driver this time, existingDriver:{} ",
            existingDriverList);
        throw new VolumeCanNotLaunchMultiDriversThisTimeExceptionThrift();
      }

      if (!volume.isEnableLaunchMultiDrivers()) {
        if (!existingDriverList.isEmpty() || request.getDriverAmount() != 1) {
          logger.warn("Volume is not allowed to launch more than one driver.");
          throw new VolumeLaunchMultiDriversExceptionThrift();
        }
      }

      for (DriverMetadata driver : existingDriverList) {
        if (driver.getDriverName().equals(request.getDriverName())) {
          logger.warn("Launch driver name already exist.");
          throw new DriverNameExistsExceptionThrift();
        }
      }

      List<DriverContainerCandidate> driverContainerCandidateList = null;
      try {
        driverContainerCandidateList = allocDriverContainer(request.getVolumeId(),
            request.getDriverAmount(), false);
      } catch (TooManyDriversExceptionThrift e) {
        logger.error("Not enough available driver container to launch driver for volume {}",
            request.getVolumeId(), e);
        throw e;
      }

      LaunchDriverResponseThrift response = new LaunchDriverResponseThrift();
      response.setRequestId(request.getRequestId());

      // check if driver container candidates exists
      if ((driverContainerCandidateList == null || driverContainerCandidateList.isEmpty())) {
        logger.error("there is no available DriverContainer");
        throw new TooManyDriversExceptionThrift();
      }

      // check if amount driver container candidates is enough
      if (driverContainerCandidateList.size() < request.getDriverAmount()) {
        logger.error("there is not enough DriverContainer: {}", driverContainerCandidateList);
        throw new TooManyDriversExceptionThrift();
      }

      /*
       * Use given host as driverContainer, support onePath now
       * if not, auto alloc driverContainer
       */
      List<DriverContainerCandidate> candidates = driverContainerCandidateList;
      List<EndPoint> endPointList = new ArrayList<>();

      // if request has set hostname
      if (request.isSetHostName()) {
        String hostName = request.getHostName();
        //check given params
        if (request.getDriverAmount() != 1) {
          logger.error("given drivenContainer is not enough,launch driver failed");
          throw new DriverAmountAndHostNotFitThrift();
        }
        //check if all host in candidates which not launched this vol before
        DriverContainerCandidate targetDriverContainer = null;

        for (DriverContainerCandidate driverContainerCheck : candidates) {
          if (hostName.equals(driverContainerCheck.getHostName())) {

            targetDriverContainer = driverContainerCheck;
          }
        }
        if (targetDriverContainer == null) {
          logger.error(
              "Failed to launch driver ,given hostname:{} current candidate driver container:{}",
              hostName, candidates);
          EndPoint failedEndPoint = new EndPoint(request.getHostName(), 0);
          endPoints.add(failedEndPoint);
          throw new DriverHostCannotUseThrift();
        }

        if (hostName.equals(targetDriverContainer.getHostName())) {
          EndPoint eps = new EndPoint(targetDriverContainer.getHostName(),
              targetDriverContainer.getPort());
          logger.debug("end-point is {}", eps);
          DriverContainerServiceBlockingClientWrapper dcClientWrapper;
          LaunchDriverRequestThrift launchDriverRequest = request.deepCopy();
          launchDriverRequest.setAccountId(request.getAccountId());
          launchDriverRequest.setSnapshotId(request.getSnapshotId());
          logger.debug("Sending launch driver request {} to {}, driver container service list{}",
              launchDriverRequest, eps, candidates);
          endPoints.add(eps);
          try {
            dcClientWrapper = driverContainerClientFactory.build(eps, 100000);
            LaunchDriverResponseThrift launchDriverResponseThrift = dcClientWrapper.getClient()
                .launchDriver(launchDriverRequest);
            Validate.isTrue(launchDriverResponseThrift.isSetRelatedDriverContainerIds());
            Validate.isTrue(launchDriverResponseThrift.getRelatedDriverContainerIdsSize() == 1);
            long relatedDriverContainerId = launchDriverResponseThrift
                .getRelatedDriverContainerIds().iterator().next();
            response.addToRelatedDriverContainerIds(relatedDriverContainerId);
            logger.warn("Succeed to launch driver {}!", targetDriverContainer);
            endPointList.add(eps);
            driverNameLaunchThisTime.add(launchDriverRequest.getDriverName());

            //save to db
            DriverMetadata driverMetadata = new DriverMetadata();
            driverMetadata.setHostName(eps.getHostName());
            driverMetadata.setDriverName(request.getDriverName());
            driverMetadata.setVolumeId(volumeId);
            driverMetadata.setDriverContainerId(relatedDriverContainerId);
            driverMetadata.setSnapshotId(request.getSnapshotId());
            driverMetadata.setDriverType(DriverType.findByName(driverThrift.name()));
            driverMetadata.setAccountId(request.getAccountId());
            driverMetadata.setDriverStatus(DriverStatus.START);
            driverMetadata.setCreateTime(System.currentTimeMillis());
            driverMetadata.setLastReportTime(System.currentTimeMillis());
            driverMetadata.setMakeUnmountForCsi(false);
            driverStore.save(driverMetadata);

          } catch (SystemMemoryIsNotEnoughThrift e) {
            logger.error("Failed to launch driver on host {} because system memory is not enough.",
                eps);
            throw e;
          } catch (SystemCpuIsNotEnoughThrift e) {
            logger
                .error("Failed to launch driver on host {} because system cpu is not enough.", eps);
            throw e;
          } catch (ExistsDriverExceptionThrift e) {
            logger.warn("Host {}, driver container already receive the same request!", eps);
            throw e;
          } catch (DriverIsUpgradingExceptionThrift e) {
            logger.warn("Host {}, driver is upgrading!", eps);
            throw e;
          } catch (DriverTypeIsConflictExceptionThrift e) {
            logger.warn("Driver type is conflict {}!", request.getDriverType());
            throw e;
          } catch (DriverNameExistsExceptionThrift e) {
            logger.warn("DriverName exists or null {}!", request.getDriverName());
            throw e;
          } catch (GenericThriftClientFactoryException e) {
            logger.error("Caught an exception", e);
            throw new NetworkErrorExceptionThrift().setDetail(e.getMessage());
          } catch (TException e) {
            logger.warn("Caught an exception");
            throw filterTexception(e);
          } catch (Exception e) {
            logger.error("launch driver failed because of\n", e);
            throw new TException(e);
          }
        }
      } else {
        //auto alloc
        /*
         * Send launch request to each driver container candidate to launch driver. If some thing
         *  wrong, the
         * launching period will stop and failed. In the case, some driver never achieved status
         * launched.
         */
        int networkErrorCount = 0;
        int systemMemoryNotEnoughErrorCount = 0;
        int driverIsUpgrading = 0;
        Queue<DriverContainerCandidate> candidateReuseQueue = new LinkedList<>();
        for (int i = 0; i < request.getDriverAmount(); ++i) {
          boolean breakStep = false;
          while (!candidates.isEmpty()) {

            int launchedDriverCount = 0;
            List<DriverMetadata> volumeBindingDrivers = driverStore.get(volumeId);
            DriverContainerCandidate candidate = candidates.remove(0);

            EndPoint eps = new EndPoint(candidate.getHostName(), candidate.getPort());
            logger.debug("end point is {}", eps);
            DriverContainerServiceBlockingClientWrapper dcClientWrapper;
            LaunchDriverRequestThrift launchDriverRequest = request.deepCopy();
            launchDriverRequest.setAccountId(request.getAccountId());
            launchDriverRequest.setSnapshotId(request.getSnapshotId());

            //if mount more than one drivers and given drivername
            //we should make it different
            if (request.getDriverAmount() > 1 && request.getDriverName() != null) {
              launchDriverRequest.setDriverName(request.getDriverName() + "_" + i);
            }

            logger.debug("Sending launch driver request {} to {}, driver container service list {}",
                launchDriverRequest, eps, candidates);

            try {
              dcClientWrapper = driverContainerClientFactory.build(eps, 100000);
              LaunchDriverResponseThrift launchDriverResponseThrift = dcClientWrapper.getClient()
                  .launchDriver(launchDriverRequest);
              Validate.isTrue(launchDriverResponseThrift.isSetRelatedDriverContainerIds());
              Validate.isTrue(launchDriverResponseThrift.getRelatedDriverContainerIdsSize() == 1);
              long relatedDriverContainerId = launchDriverResponseThrift
                  .getRelatedDriverContainerIds().iterator().next();
              response.addToRelatedDriverContainerIds(relatedDriverContainerId);
              logger.warn("Succeed to launch driver {}!", candidate);
              endPointList.add(eps);
              endPoints.add(eps);
              driverNameLaunchThisTime.add(launchDriverRequest.getDriverName());

              //save to db
              DriverMetadata driverMetadata = new DriverMetadata();
              driverMetadata.setHostName(eps.getHostName());
              driverMetadata.setDriverName(launchDriverRequest.getDriverName());
              driverMetadata.setVolumeId(volumeId);
              driverMetadata.setDriverContainerId(relatedDriverContainerId);
              driverMetadata.setSnapshotId(request.getSnapshotId());
              driverMetadata.setDriverType(DriverType.findByName(driverThrift.name()));
              driverMetadata.setAccountId(request.getAccountId());
              driverMetadata.setDriverStatus(DriverStatus.START);
              driverMetadata.setCreateTime(System.currentTimeMillis());
              driverMetadata.setLastReportTime(System.currentTimeMillis());
              driverMetadata.setMakeUnmountForCsi(false);
              driverStore.save(driverMetadata);

              break;
            } catch (SystemMemoryIsNotEnoughThrift e) {
              ++systemMemoryNotEnoughErrorCount;
              logger.warn("failed to launch driver on {}, system is out of memory, try next!", eps);
            } catch (ExistsDriverExceptionThrift e) {
              logger.warn("{}, driver container already receive the same request!", eps);
              candidateReuseQueue.add(candidate);
            } catch (DriverIsUpgradingExceptionThrift e) {
              ++driverIsUpgrading;
              logger.warn("Host {}, driver is upgrading, try next!", eps);
            } catch (DriverTypeIsConflictExceptionThrift e) {
              logger.warn("Driver type is conflict {}!", request.getDriverType());
            } catch (DriverNameExistsExceptionThrift e) {
              logger.warn("DriverName exists or null {}!", request.getDriverName());
              throw e;
            } catch (TException e) {
              ++networkErrorCount;
              logger.warn("Failed to launch driver on {}, because of Exception: {}, try next!", eps,
                  e);
            } catch (Exception e) {
              logger.warn("Failed to launch driver on {}, because of Exception: {}, try next!", eps,
                  e);
            }
          }
          while (!candidateReuseQueue.isEmpty()) {
            candidates.add(candidateReuseQueue.poll());
          }

          if (breakStep) {
            break; //break for
          }
        }

        if (endPointList.size() < request.getDriverAmount()) {
          if (networkErrorCount > 0) {
            logger.error("Failed to launch all drivers because of network error happened.");
            throw new TTransportException(
                "Failed to launch all drivers because of network error happened.");
          } else if (systemMemoryNotEnoughErrorCount > 0) {
            logger.error(
                "Failed to launch all drivers because some driver containers are lack of memory "
                    + "to launch the drivers.");
            throw new SystemMemoryIsNotEnoughThrift();
          } else if (driverIsUpgrading > 0) {
            logger.error("Failed to launch all drivers because some drivers are upgrading.");
            throw new DriverIsUpgradingExceptionThrift();
          } else {
            logger.error("Failed to launch all drivers because some other reasons.");
            throw new TooManyDriversExceptionThrift();
          }
        }
      }

      if (CollectionUtils.isNotEmpty(endPointList)) {
        response.setDriverNameLaunchThisTime(driverNameLaunchThisTime);
        logger.warn(
            "launchDriver for volume :{}, this time the launch endPoint size:{}, value:{}, the "
                + "name:{}",
            volumeId, endPointList.size(), endPointList, driverNameLaunchThisTime);
      }
      logger.warn("launchDriver response: {}", response);
      return response;
    } catch (TException e) {
      logger.error("launchDriver for volume :{}, caught an exception", volumeId, e);
      isException = true;
      throw filterTexception(e);
    } finally {
      snapshotLock.writeLock().unlock();
      try {
        logger.warn("is caught exception : {}", isException);
       
        if (isException) {
          buildFailedOperationWrtLaunchDriverAndSaveToDb(request.getAccountId(),
              request.getVolumeId(),
              OperationType.LAUNCH, volumeName, driverName + " * " + request.getDriverAmount(),
              endPoints);
        } else {
          if (!driverNameLaunchThisTime.isEmpty()) {
            buildActiveOperationWrtLaunchDriverAndSaveToDb(request.getAccountId(),
                request.getVolumeId(),
                OperationType.LAUNCH, volumeName, driverName + " * " + request.getDriverAmount(),
                endPoints);
          }
        }
      } finally {
        if (lock != null) {
          lock.setVolumeOperation(LockForSaveVolumeInfo.BusySource.LOCK_WAITING);
          lock.unlock();
        }
      }
    }
  }

  @Override
  public UmountDriverResponseThrift umountDriver(UmountDriverRequestThrift request)
      throws AccessDeniedExceptionThrift, VolumeNotFoundExceptionThrift,
      ServiceHavingBeenShutdownThrift,
      ExistsClientExceptionThrift, DriverIsLaunchingExceptionThrift,
      SnapshotRollingBackExceptionThrift,
      DriverLaunchingExceptionThrift,
      DriverUnmountingExceptionThrift, VolumeDeletingExceptionThrift, InvalidInputExceptionThrift,
      DriverIsUpgradingExceptionThrift, TransportExceptionThrift,
      DriverContainerIsIncExceptionThrift,
      InvalidInputExceptionThrift, PermissionNotGrantExceptionThrift,
      AccountNotFoundExceptionThrift,
      ServiceIsNotAvailableThrift, NoDriverLaunchExceptionThrift, VolumeInExtendingExceptionThrift,
      VolumeCyclingExceptionThrift, VolumeNotAvailableExceptionThrift,
      VolumeIsBeginMovedExceptionThrift,
      VolumeIsCloningExceptionThrift, VolumeIsMovingExceptionThrift,
      VolumeInMoveOnlineDoNotHaveOperationExceptionThrift, VolumeIsCopingExceptionThrift,
      DriverStillReportExceptionThrift, TException {

    //check the instance status
    checkInstanceStatus("umountDriver");
    logger.warn("umountDriver request: {}", request);

    securityManager.hasPermission(request.getAccountId(), "umountDriver");
    securityManager.hasRightToAccess(request.getAccountId(), request.getVolumeId());

    long volumeId = request.getVolumeId();

    boolean forceUmount = false;
    UmountDriverResponseThrift response = new UmountDriverResponseThrift(request.getRequestId(),
        null);

    //for force umount driver
    if (request.isSetForceUmount() && request.isForceUmount()) {
      logger.warn("when umountDriver for volume :{}, in force umount step", request.getVolumeId());
      forceUmount = true;
    }

    // this section for searching driver type name and volume name, used for operation save
    try {
      VolumeMetadata volume = null;
      List<DriverMetadata> driverMetadataList = new ArrayList<>();
      try {
        VolumeMetadataAndDrivers volumeAndDriverInfo = volumeInformationManger
            .getDriverContainVolumes(request.getVolumeId(), request.getAccountId(), false);

        volume = volumeAndDriverInfo.getVolumeMetadata();
        driverMetadataList = volumeAndDriverInfo.getDriverMetadatas();
        logger.warn("got drivers:{} binding with volume:{}", driverMetadataList, volumeId);
      } catch (VolumeNotFoundException e) {
        logger.error("when umountDriver, can not find the volume :{}", request.getVolumeId());
      }

      //check the Operation
      if (volume != null && !forceUmount) {
        checkExceptionForOperation(volume, OperationFunctionType.umountDriver);
      }

      if (driverMetadataList.isEmpty()) {
        throw new NoDriverLaunchExceptionThrift().setDetail("request: " + request);
      }

     
      Set<String> canUmountDriverNames = new HashSet<>();
      String volumeName = "notFind";
      if (volume != null) {
        volumeName = volume.getName();
      }
      List<String> umountDriverNames = new ArrayList<>();
      // end searching driver type and volume name, volumeAndDriverTypeName

      List<DriverIpTargetThrift> driverIpTargetReq = request.getDriverIpTargetList();
      // List<DriverIpTargetThrift> driverIpTargetList = new
      // ArrayList<DriverIpTargetThrift>();

      // umount all drivers binding to specified volume due to no drivers
      // specified
      if (driverIpTargetReq == null || driverIpTargetReq.size() == 0) {
        driverIpTargetReq = new ArrayList<>();
        request.setDriverIpTargetList(driverIpTargetReq);
        logger.warn("No driver ip specified in request, to umount all drivers of volumeId {}.",
            volumeId);

        for (DriverMetadata driver : driverMetadataList) {
          DriverIpTargetThrift oneTarget = new DriverIpTargetThrift(driver.getSnapshotId(),
              driver.getHostName(),
              DriverTypeThrift.findByValue(driver.getDriverType().getValue()),
              driver.getDriverContainerId());
          driverIpTargetReq.add(oneTarget);
        }
        if (driverIpTargetReq.isEmpty()) {
          logger.warn("No driver to umount for volume:{}, the response: {}", volumeId, response);
          return response;
        }
      }

      // end point list used for operation save
      List<EndPoint> endPointList = new ArrayList<>();
      Iterator<DriverIpTargetThrift> iterator = driverIpTargetReq.iterator();
      while (iterator.hasNext()) {
        DriverIpTargetThrift driver = iterator.next();
        String driverType = driver.getDriverType().name();
        DriverMetadata driverMetadata = driverStore.get(driver.getDriverContainerId(), volumeId,
            DriverType.findByName(driverType), driver.getSnapshotId());

        //for force umount driver, just remove the value in db
        if (forceUmount) {
          //delete in driverAndNodeStore
          if (driverMetadata != null) {
            logger.warn("when umountDriver force for driver :{}, name:{}, remove nodeId in db",
                driver, driverMetadata.getDriverName());

            //just remove the driver which have long time not report
            long currentTime = System.currentTimeMillis();
            long lastReportTime = driverMetadata.getLastReportTime();
            //30s
            if (currentTime - lastReportTime > 30 * 1000) {
              logger.warn(
                  "when umountDriver force for driver :{}, not report more then 30s, so can remove",
                  driver);
              //delete in driverStore
              driverStore.delete(driver.getDriverContainerId(), volumeId,
                  DriverType.findByName(driverType), driver.getSnapshotId());

              //delete iscsi
              iscsiRuleRelationshipStore
                  .deleteByDriverKey(
                      new DriverKey(driver.getDriverContainerId(), volumeId, driver.getSnapshotId(),
                          DriverType.findByName(driverType)));
            } else {
              logger.warn(
                  "when umountDriver force for driver :{}, still have report:{}, so can not remove",
                  driver, lastReportTime);
              throw new DriverStillReportExceptionThrift();
            }
          }
        }

        if (driverMetadata == null) {
          logger.warn("when umountDriver for volume :{}, can not find the driver :{}",
              request.getVolumeId(), driver);
        }

        //can umount
        if (driverType.equals("NBD")) {
          umountDriverNames.add("PYD");
        } else {
          umountDriverNames.add(driverType);
        }
        EndPoint endPoint = new EndPoint();
        endPoint.setHostName(driver.getDriverIp());
        endPointList.add(endPoint);
      }

      logger.warn("To find driver container for each driver to umount,and the endPointList is:{}",
          endPointList);
      if (driverIpTargetReq.isEmpty()) {
        logger.warn(
            "driver still have node, this time no driver to umount for volume:{}, the response:{}",
            volumeId, response);
        return response;
      }

      Set<Instance> driverContainers = new HashSet<>();
      Set<DriverIpTargetThrift> driverContainerNotFind = new HashSet<>();
      Set<Instance> driverContainerInstances = instanceStore
          .getAll(PyService.DRIVERCONTAINER.getServiceName());
      logger.info("Existing driver containers {}", driverContainerInstances);
      boolean driverContainerIsInc = false;
      if (!forceUmount) {
        for (DriverIpTargetThrift driverIp : driverIpTargetReq) {
          boolean getDriverContainer = false;
          for (Instance instance : driverContainerInstances) {
            if (instance.getId().getId() == driverIp.getDriverContainerId()) {
              if (instance.getStatus() == InstanceStatus.HEALTHY) {
                driverContainers.add(instance);
              } else {
                logger.warn("umountDriver for volume:{} find the driverContainerIsInc:{}", volumeId,
                    instance);
                driverContainerIsInc = true;
              }

              getDriverContainer = true;
            }
          }

          if (!getDriverContainer) {
            driverContainerNotFind.add(driverIp);
          }
        }
      }

      //by the umountDriver ip can not find any driverContainer, may the driverContainerid change
      // (wipeout and install)
      if (driverContainers.isEmpty() && !driverContainerIsInc && !forceUmount) {
        logger.error("Something wrong when umount Driver, may be the driverContainer id change,"
                + "the want to umount:{}, and all driverContainer:{} ", driverIpTargetReq,
            driverContainerInstances);
        throw new FailedToUmountDriverExceptionThrift();
      }

      logger.debug("Umount drivers on driver containers {}", driverContainers);
      for (Instance driverContainer : driverContainers) {
        try {
          final DriverContainerServiceBlockingClientWrapper dcClientWrapper =
              driverContainerClientFactory
                  .build(driverContainer.getEndPoint(), 50000);
          UmountDriverRequestThrift requestToOneDc = new UmountDriverRequestThrift(request);
          requestToOneDc.getDriverIpTargetList().clear();
          for (DriverIpTargetThrift driverOne : driverIpTargetReq) {
            if (driverOne.getDriverContainerId() == driverContainer.getId().getId()) {
              requestToOneDc.addToDriverIpTargetList(driverOne);
            }
          }

          logger.warn("umountDriver request_ToOneDC:{} to driver container:{}", requestToOneDc,
              driverContainer.getEndPoint());
          UmountDriverResponseThrift umountDriverResponse = dcClientWrapper.getClient()
              .umountDriver(requestToOneDc);
          if (umountDriverResponse.isSetDriverIpTarget()) {
            List<DriverIpTargetThrift> allDrivers = requestToOneDc.getDriverIpTargetList();
            for (DriverIpTargetThrift failedDriver : umountDriverResponse.getDriverIpTarget()) {
              failedDriver.setDriverIp(driverContainer.getEndPoint().getHostName());
              allDrivers.remove(failedDriver);
            }
          }
          logger.warn("the end umountDriver request_ToOneDC:{} to driver container:{}",
              requestToOneDc,
              driverContainer.getEndPoint());

         
          try {
            markDriverStatus(request.getVolumeId(), requestToOneDc.getDriverIpTargetList());
          } catch (Exception e) {
            logger.warn("notify info center driver failed:", e);
          }

          //releaseDriverContainer
          synchronized (driverStore) {
            for (DriverIpTargetThrift driverIpTargetThrift : requestToOneDc
                .getDriverIpTargetList()) {
              DriverType driverType = DriverType
                  .valueOf(driverIpTargetThrift.getDriverType().name());
              driverStore
                  .delete(driverIpTargetThrift.getDriverContainerId(), request.getVolumeId(),
                      driverType,
                      driverIpTargetThrift.getSnapshotId());
            }
          }

        } catch (ExistsClientExceptionThrift e) {
          logger.error("Caught an exception", e);
          throw e;
        } catch (DriverIsLaunchingExceptionThrift e) {
          logger.error("Caught an exception", e);
          throw e;
        } catch (TTransportException e) {
          logger.error("Caught an exception", e);
          throw new TransportExceptionThrift().setDetail(e.getMessage());
        } catch (GenericThriftClientFactoryException e) {
          logger.error("Caught an exception", e);
          throw new NetworkErrorExceptionThrift().setDetail(e.getMessage());
        } catch (TException e) {
          logger.error("Caught an exception", e);
          throw filterTexception(e);
        } catch (Exception e) {
          logger.error("Caught an exception when umount driver on {}", driverContainer, e);
          throw new TException(e);
        }
      }
      if (!driverContainerNotFind.isEmpty()) {
        logger.warn("when umountDriver, can not find the driverContainers{} for volume :{}",
            driverContainerNotFind,
            volumeId);
        throw new TException();
      }

      if (driverContainerIsInc) {
        logger.warn("when umountDriver for volume :{}, have some driverContainerIsInc", volumeId);
        throw new DriverContainerIsIncExceptionThrift();
      }
      // begin add end-operation by umount driver due to syn
      logger.debug("begin save umount-volume operation");
      buildEndOperationWrtUmountDriverAndSaveToDb(request.getAccountId(), request.getVolumeId(),
          OperationType.UMOUNT,
          volumeName, umountDriverNames.stream().collect(Collectors.joining(", ")), endPointList);

      logger.warn("umountDriver response: {}", response);
      return response;
    } finally {
      logger.warn("umountDriver end :{}", response.getRequestId());
    }
  }

  /**
   * An implementation of interface
   * {@link InformationCenter.Iface#createVolumeAccessRules(CreateVolumeAccessRulesRequest)}
   * which create an specified access rule in request to database.
   *
   * <p>Status of a new access rule is always "available".
   */
  @Override
  public synchronized CreateVolumeAccessRulesResponse createVolumeAccessRules(
      CreateVolumeAccessRulesRequest request)
      throws VolumeAccessRuleDuplicateThrift, ServiceIsNotAvailableThrift,
      InvalidInputExceptionThrift,
      TException {

    //check the instance status
    checkInstanceStatus("createVolumeAccessRules");

    logger.warn("createVolumeAccessRules request: {}", request);
    securityManager.hasPermission(request.getAccountId(), "createVolumeAccessRules");

    CreateVolumeAccessRulesResponse response = new CreateVolumeAccessRulesResponse();
    response.setRequestId(request.getRequestId());

    long ruleId = 0;
    if (request.getAccessRules() == null || request.getAccessRulesSize() == 0) {
      logger.warn("No access rule to create in request, nothing to do");
      return response;
    }

    // checkout each of the rule that were stored in the request to find out
    // whether there are some rules duplicated
    List<VolumeAccessRuleThrift> volumeAccessRuleThriftList = request.getAccessRules();
    for (VolumeAccessRuleThrift volumeAccessRuleThrift : volumeAccessRuleThriftList) {
      int counter = 0;
      for (VolumeAccessRuleThrift volumeAccessRuleThriftTemp : volumeAccessRuleThriftList) {
        if (volumeAccessRuleThrift.equals(volumeAccessRuleThriftTemp)) {
          counter++;
        }
      }

      if (counter > 1) {
        logger.error("when createVolumeAccessRules, find the InvalidInput exception");
        throw new InvalidInputExceptionThrift();
      }
    }

    // get access rule list from accessRuleStore
    List<AccessRuleInformation> rulesFromDb = accessRuleStore.list();

    for (VolumeAccessRuleThrift volumeAccessRuleThrift : volumeAccessRuleThriftList) {
      boolean accessRuleExisted = false;
      for (AccessRuleInformation rule : rulesFromDb) {
        if (rule.getIpAddress().equals(volumeAccessRuleThrift.getIncomingHostName())
            && rule.getPermission() == volumeAccessRuleThrift.getPermission().getValue()) {
          accessRuleExisted = true;
          ruleId = rule.getRuleId();
          break;
        } else {
          // do nothing
        }
      }

      if (!accessRuleExisted) {
        logger.debug("information center going to save the access rules");
        VolumeAccessRule volumeAccessRule = RequestResponseHelper
            .buildVolumeAccessRuleFrom(volumeAccessRuleThrift);
        // status "available" for a new access rule
        volumeAccessRule.setStatus(AccessRuleStatus.AVAILABLE);
        accessRuleStore.save(volumeAccessRule.toAccessRuleInformation());
      } else {
        // throw an exception out
        logger.debug("information center throws an exception VolumeAccessRuleDuplicateThrift");
        VolumeAccessRuleDuplicateThrift volumeAccessRuleDuplicateThrift =
            new VolumeAccessRuleDuplicateThrift();
        volumeAccessRuleDuplicateThrift.setDetail(String.valueOf(ruleId));
        throw volumeAccessRuleDuplicateThrift;
      }
    }
    String hostname = request.getAccessRules().get(0).getIncomingHostName();
    buildEndOperationWrtCreateVolumeAccessRulesAndSaveToDb(request.getAccountId(), hostname);
    logger.warn("createVolumeAccessRules response: {}", response);
    return response;
  }

  /**
   * An implementation of inter   e
   * {@link ControlCenter.Iface#deleteVolumeAccessRules(DeleteVolumeAccessRulesRequest)}
   * to delete volume access rules. This is an atomic transaction which contains coordination
   * between driver container and infocenter. Node control center plays a role as coordinator.
   */
  @Override
  public DeleteVolumeAccessRulesResponse deleteVolumeAccessRules(
      DeleteVolumeAccessRulesRequest request)
      throws ServiceHavingBeenShutdownThrift, FailedToTellDriverAboutAccessRulesExceptionThrift,
      PermissionNotGrantExceptionThrift, ServiceIsNotAvailableThrift,
      AccountNotFoundExceptionThrift,
      TException {

    //check the instance status
    checkInstanceStatus("deleteVolumeAccessRules");
    logger.warn("deleteVolumeAccessRules request: {}", request);

    securityManager.hasPermission(request.getAccountId(), "deleteVolumeAccessRules");
    ListVolumeAccessRulesResponse listVolumeAccessRulesResponse;
    try {

      listVolumeAccessRulesResponse = listVolumeAccessRules(
          new ListVolumeAccessRulesRequest(request.getRequestId(), request.getAccountId()));
    } catch (TException e) {
      logger.error("Caught an exception", e);
      throw e;
    } catch (Exception e) {
      logger.error("Caught an exception", e);
      throw new TException(e);
    }

    List<String> hostnameList = new ArrayList<>();
    for (VolumeAccessRuleThrift volumeAccessRuleThrift : listVolumeAccessRulesResponse
        .getAccessRules()) {
      if (request.getRuleIds().contains(volumeAccessRuleThrift.getRuleId())) {
        hostnameList.add(volumeAccessRuleThrift.getIncomingHostName());
      }
    }

    request.setCommit(false);
    DeleteVolumeAccessRulesResponse deleteVolumeAccessRulesResponse;

    deleteVolumeAccessRulesResponse = beginDeleteVolumeAccessRules(request.getRequestId(),
        request.getRuleIds(),
        request.isCommit());
    if (deleteVolumeAccessRulesResponse.getAirAccessRuleList() != null
        && !deleteVolumeAccessRulesResponse
        .getAirAccessRuleList().isEmpty()) {
      logger.warn(
          "Unable to delete access rules in request, due to exist air access rules {} which are "
              + "operating by other action",
          deleteVolumeAccessRulesResponse.getAirAccessRuleList());
      logger.warn("deleteVolumeAccessRules response: {}", deleteVolumeAccessRulesResponse);
      return deleteVolumeAccessRulesResponse;
    }

    request.setCommit(true);
    logger.warn("deleteVolumeAccessRules request: {}", request);
    DeleteVolumeAccessRulesResponse response;

    response = deleteVolumeAccessRulesResponse = beginDeleteVolumeAccessRules(
        request.getRequestId(),
        request.getRuleIds(), request.isCommit());

    buildEndOperationWrtDeleteVolumeAccessRulesAndSaveToDb(request.getAccountId(),
        hostnameList.stream().collect(Collectors.joining(", ")));
    logger.warn("deleteVolumeAccessRules response: {}", response);
    return response;
  }


  
  public DeleteVolumeAccessRulesResponse beginDeleteVolumeAccessRules(long requestId,
      List<Long> ruleIds,
      boolean commit) {
    // prepare a response for deleting volume access rule request
    DeleteVolumeAccessRulesResponse response = new DeleteVolumeAccessRulesResponse(requestId);

    if (ruleIds == null || ruleIds.size() == 0) {
      logger.debug("No access rules existing in request to delete");
      return response;
    }

    for (long ruleId : ruleIds) {
      AccessRuleInformation accessRuleInformation = accessRuleStore.get(ruleId);
      if (accessRuleInformation == null) {
        logger.debug("No access rule with id {}", ruleId);
        continue;
      }
      VolumeAccessRule volumeAccessRule = new VolumeAccessRule(accessRuleInformation);

      List<Volume2AccessRuleRelationship> relationshipList = new ArrayList<>();
      List<VolumeRuleRelationshipInformation> relationshipInfoList = volumeRuleRelationshipStore
          .getByRuleId(ruleId);
      if (relationshipInfoList != null && relationshipInfoList.size() > 0) {
        for (VolumeRuleRelationshipInformation relationshipInfo : relationshipInfoList) {
          relationshipList.add(new Volume2AccessRuleRelationship(relationshipInfo));
        }
      } else {
        logger.debug("Rule {} is not applied before", volumeAccessRule);
      }

      // a flag used to check if the access rule is able to be deleted
      boolean existAirAccessRule = false;
      for (Volume2AccessRuleRelationship relationship : relationshipList) {

        
        switch (relationship.getStatus()) {
          case FREE:
            // do nothing
            break;
          case APPLIED:
            // do nothing
            break;
          case APPLING:
          case CANCELING:
            existAirAccessRule = true;
            break;
          default:
            logger.warn("Unknown status {} of relationship", relationship.getStatus());
            break;
        }

        if (existAirAccessRule) {
          // unable to delete the access rule, so just break
          break;
        }
      }

      if (existAirAccessRule) {
        logger.debug("Access rule {} already has an operation on it before deleting",
            volumeAccessRule);
        response.addToAirAccessRuleList(
            RequestResponseHelper.buildVolumeAccessRuleThriftFrom(volumeAccessRule));
        continue;
      }

      if (!commit) {
        if (volumeAccessRule.getStatus() != AccessRuleStatus.DELETING) {
          volumeAccessRule.setStatus(AccessRuleStatus.DELETING);
          accessRuleStore.save(volumeAccessRule.toAccessRuleInformation());
        } else {
          // do nothing
        }

        continue;
      }

      // delete the volume access rule and relationship with volume from
      // database if exists
      accessRuleStore.delete(ruleId);
      volumeRuleRelationshipStore.deleteByRuleId(ruleId);
    }

    logger.warn("deleteVolumeAccessRules response: {}", response);
    return response;
  }

  
  @Override
  public GetVolumeAccessRulesResponse getVolumeAccessRules(GetVolumeAccessRulesRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {

    //check the instance status
    checkInstanceStatus("getVolumeAccessRules");
    logger.warn("getVolumeAccessRules request: {}", request);

    GetVolumeAccessRulesResponse response = new GetVolumeAccessRulesResponse();
    try {

      response.setRequestId(request.getRequestId());
      response.setAccessRules(new ArrayList<>());

      List<VolumeRuleRelationshipInformation> relationshipInfoList = volumeRuleRelationshipStore
          .getByVolumeId(request.getVolumeId());
      if (relationshipInfoList == null || relationshipInfoList.size() == 0) {
        logger.debug("The volume {} hasn't be applied any access rules", request.getVolumeId());
        return response;
      }

      List<Volume2AccessRuleRelationship> relationshipList =
          new ArrayList<Volume2AccessRuleRelationship>();
      for (VolumeRuleRelationshipInformation relationshipInfo : relationshipInfoList) {
        Volume2AccessRuleRelationship relationship = new Volume2AccessRuleRelationship(
            relationshipInfo);
        relationshipList.add(relationship);
      }

      for (Volume2AccessRuleRelationship relationship : relationshipList) {
        VolumeAccessRule accessRule = new VolumeAccessRule(
            accessRuleStore.get(relationship.getRuleId()));
        // build access rule to remote
        VolumeAccessRuleThrift accessRuleToRemote = RequestResponseHelper
            .buildVolumeAccessRuleThriftFrom(accessRule);
        accessRuleToRemote
            .setStatus(AccessRuleStatusThrift.valueOf(relationship.getStatus().name()));
        response.addToAccessRules(accessRuleToRemote);
      }

    } catch (Exception e) {
      logger.error("Caught an exception", e);
      throw new TException(e);
    }

    logger.warn("getVolumeAccessRules response: {}", response);
    return response;
  }

  /**
   * An implementation of    erface
   * {@link InformationCenter.Iface#applyVolumeAccessRules(ApplyVolumeAccessRulesRequest)}
   * which apply specified access rule to the volume in request.
   *
   * <p>If exist some air rules which means unable to apply those rules to specified volume in
   * request, add those rules in air list of response.
   *
   */
  public ApplyVolumeAccessRulesResponse beginApplyVolumeAccessRules(
      ApplyVolumeAccessRulesRequest request,
      VolumeMetadata volume) throws ApplyFailedDueToVolumeIsReadOnlyExceptionThrift {

    ApplyVolumeAccessRulesResponse response = new ApplyVolumeAccessRulesResponse(
        request.getRequestId());
    List<Long> toApplyRuleIdList = request.getRuleIds();
    if (toApplyRuleIdList == null || toApplyRuleIdList.isEmpty()) {
      logger.warn("No rule exists in applying request, do nothing");
      return response;
    }

    boolean needThrowException = false;
    String exceptionDetail = new String();
    for (long toApplyRuleId : toApplyRuleIdList) {
      VolumeAccessRule accessRule = new VolumeAccessRule(accessRuleStore.get(toApplyRuleId));

      // unable to apply a deleting access rule to volume
      if (accessRule.getStatus() == AccessRuleStatus.DELETING) {
        logger.warn("The access rule {} is deleting now, unable to apply this rule to volume {}",
            accessRule,
            request.getVolumeId());
        response.addToAirAccessRuleList(
            RequestResponseHelper.buildVolumeAccessRuleThriftFrom(accessRule));
        continue;
      }

      //the volume READONLY
      if (volume.getReadWrite().equals(VolumeMetadata.ReadWriteType.READONLY) && !accessRule
          .getPermission()
          .equals(AccessPermissionType.READ)) {
        needThrowException = true;
        exceptionDetail += "Failed: " + accessRule + "\n";
        continue;
      }

      // turn relationship info get from db to relationship structure
      List<Volume2AccessRuleRelationship> relationshipList = new ArrayList<>();
      List<VolumeRuleRelationshipInformation> relationshipInfoList = volumeRuleRelationshipStore
          .getByRuleId(toApplyRuleId);
      if (relationshipInfoList != null && relationshipInfoList.size() > 0) {
        for (VolumeRuleRelationshipInformation relationshipInfo : relationshipInfoList) {
          Volume2AccessRuleRelationship relationship = new Volume2AccessRuleRelationship(
              relationshipInfo);
          relationshipList.add(relationship);
        }
      }

      
      boolean existInRelationshipStore = false;
      Volume2AccessRuleRelationship relationship2Apply = new Volume2AccessRuleRelationship();
      for (Volume2AccessRuleRelationship relationship : relationshipList) {
        if (relationship.getVolumeId() == request.getVolumeId()) {
          existInRelationshipStore = true;
          relationship2Apply = relationship;
        }
      }
      if (!existInRelationshipStore) {
        logger.debug("The rule {} is not being applied to the volume {} before", accessRule,
            request.getVolumeId());
        relationship2Apply = new Volume2AccessRuleRelationship();
        relationship2Apply.setRelationshipId(RequestIdBuilder.get());
        relationship2Apply.setRuleId(toApplyRuleId);
        relationship2Apply.setVolumeId(request.getVolumeId());
        relationship2Apply.setStatus(AccessRuleStatusBindingVolume.FREE);
        relationshipList.add(relationship2Apply);
      }

      /*
       * Turn all relationship to a proper status base on state machine below.
       */
      // A state machine to transfer status of relationship to proper
      // status.
      switch (relationship2Apply.getStatus()) {
        case FREE:
          if (request.isCommit()) {
            relationship2Apply.setStatus(AccessRuleStatusBindingVolume.APPLIED);
          } else {
            relationship2Apply.setStatus(AccessRuleStatusBindingVolume.APPLING);
          }
          // status of relationship has changed, save it to db
          logger.debug("FREE, save {}", relationship2Apply.getStatus());
          volumeRuleRelationshipStore
              .save(relationship2Apply.toVolumeRuleRelationshipInformation());
          break;
        case APPLING:
          if (request.isCommit()) {
            relationship2Apply.setStatus(AccessRuleStatusBindingVolume.APPLIED);
            logger.debug("APPLYING , save {}", relationship2Apply.getStatus());
            volumeRuleRelationshipStore
                .save(relationship2Apply.toVolumeRuleRelationshipInformation());
          }
          break;
        case APPLIED:
          logger.debug("APPLIED, DO NOTHING");
          // do nothing
          break;
        case CANCELING:
          logger.debug("CANCELING, ERROR");
          VolumeAccessRuleThrift accessRuleToRemote = RequestResponseHelper
              .buildVolumeAccessRuleThriftFrom(accessRule);
          accessRuleToRemote
              .setStatus(AccessRuleStatusThrift.valueOf(relationship2Apply.getStatus().name()));
          response.addToAirAccessRuleList(accessRuleToRemote);
          break;
        default:
          logger.warn("Unknown status {}", relationship2Apply.getStatus());
          break;
      }
    }

    if (needThrowException) {
      ApplyFailedDueToVolumeIsReadOnlyExceptionThrift exception =
          new ApplyFailedDueToVolumeIsReadOnlyExceptionThrift();
      exception.setDetail(exceptionDetail);
      logger.warn("when beginApplyVolumeAccessRules, find exception :{}", exception);
      throw exception;
    }

    logger.warn("beginApplyVolumeAccessRules response: {}", response);
    return response;
  }

  /**
   * This method is an implementatio   f interface
   * {@link ControlCenter.Iface#applyVolumeAccessRules(ApplyVolumeAccessRulesRequest)}
   * to apply access rules to expect volume. Application is an atomic transaction which is composed
   * by one process to infocenter and another to driver container. Commit this application action to
   * infocenter only if the aomic transaction is done well. Otherwise, no commit to infocenter and
   * throw an properly exception.
   */
  @Override
  public ApplyVolumeAccessRulesResponse applyVolumeAccessRules(
      ApplyVolumeAccessRulesRequest request)
      throws FailedToTellDriverAboutAccessRulesExceptionThrift, VolumeNotFoundExceptionThrift,
      VolumeBeingDeletedExceptionThrift, ApplyFailedDueToVolumeIsReadOnlyExceptionThrift,
      PermissionNotGrantExceptionThrift, ServiceHavingBeenShutdownThrift,
      ServiceIsNotAvailableThrift,
      AccessDeniedExceptionThrift, InvalidInputExceptionThrift, TException {

    //check the instance status
    checkInstanceStatus("applyVolumeAccessRules");

    request.setCommit(false);
    logger.warn("applyVolumeAccessRules request: {}", request);

    VolumeMetadata volume;
    List<DriverMetadata> driverList;
    try {
      VolumeMetadataAndDrivers volumeAndDriverInfo = volumeInformationManger
          .getDriverContainVolumes(request.getVolumeId(), request.getAccountId(), false);

      volume = volumeAndDriverInfo.getVolumeMetadata();
      driverList = volumeAndDriverInfo.getDriverMetadatas();
    } catch (VolumeNotFoundException e) {
      logger
          .error("when applyVolumeAccessRules, can not find the volume :{}", request.getVolumeId());
      throw new VolumeNotFoundExceptionThrift();
    }

    if (volume.isDeletedByUser()) {
      throw new VolumeBeingDeletedExceptionThrift();
    }

    ApplyVolumeAccessRulesResponse applyVolumeAccessRulesResponse;
    // ApplyVolumeAccessRules
    applyVolumeAccessRulesResponse = beginApplyVolumeAccessRules(request, volume);

    if (applyVolumeAccessRulesResponse.getAirAccessRuleList() != null
        && !applyVolumeAccessRulesResponse
        .getAirAccessRuleList().isEmpty()) {
      logger.warn(
          "Unable to apply access rules to specified volume in request, due to exist other "
              + "operation on the rules {}",
          applyVolumeAccessRulesResponse.getAirAccessRuleList());
      return applyVolumeAccessRulesResponse;
    }

    if (driverList.size() == 0 || driverList.get(0).getDriverType() != DriverType.ISCSI) {
      // apply the volume access rules to information center, if any
      // exception throw, throw the exception to
      // client
      request.setCommit(true); // apply volume access rule successfully
      // and commit it to infocenter
      logger.warn("applyVolumeAccessRules request: {}", request);
      applyVolumeAccessRulesResponse = beginApplyVolumeAccessRules(request, volume);
      ListVolumeAccessRulesResponse response = listVolumeAccessRules(
          new ListVolumeAccessRulesRequest(request.getRequestId(), request.getAccountId()));

      List<String> hostnameList = new ArrayList<>();
      for (VolumeAccessRuleThrift volumeAccessRuleThrift : response.getAccessRules()) {
        if (request.getRuleIds().contains(volumeAccessRuleThrift.getRuleId())) {
          hostnameList.add(volumeAccessRuleThrift.getIncomingHostName());
        }
      }
      buildEndOperationWrtApplyVolumeAccessRulesAndSaveToDb(request.getAccountId(),
          request.getVolumeId(),
          volume.getName(), hostnameList.stream().collect(Collectors.joining(", ")));
      logger.warn("applyVolumeAccessRules response: {}", applyVolumeAccessRulesResponse);
      return applyVolumeAccessRulesResponse;
    }

    request.setCommit(true);
    logger.warn("applyVolumeAccessRules request: {}", request);
    applyVolumeAccessRulesResponse = beginApplyVolumeAccessRules(request, volume);

    ListVolumeAccessRulesRequest listVolumeAccessRulesRequest = new ListVolumeAccessRulesRequest(
        request.getRequestId(), request.getAccountId());

    ListVolumeAccessRulesResponse response = listVolumeAccessRules(listVolumeAccessRulesRequest);

    List<String> hostnameList = new ArrayList<>();
    for (VolumeAccessRuleThrift volumeAccessRuleThrift : response.getAccessRules()) {
      if (request.getRuleIds().contains(volumeAccessRuleThrift.getRuleId())) {
        hostnameList.add(volumeAccessRuleThrift.getIncomingHostName());
      }
    }

    buildEndOperationWrtApplyVolumeAccessRulesAndSaveToDb(request.getAccountId(),
        request.getVolumeId(),
        volume.getName(), hostnameList.stream().collect(Collectors.joining(", ")));
    logger.warn("applyVolumeAccessRules response: {}", applyVolumeAccessRulesResponse);
    return applyVolumeAccessRulesResponse;
  }

  /**
   * This is an implementation of interface to cancel volume access rules from specified volume.
   * This is an atomic transaction
   */
  @Override
  public CancelVolAccessRuleAllAppliedResponse cancelVolAccessRuleAllApplied(
      CancelVolAccessRuleAllAppliedRequest request)
      throws AccessRuleNotAppliedThrift, PermissionNotGrantExceptionThrift,
      AccessRuleUnderOperationThrift,
      AccessRuleNotFoundThrift, AccountNotFoundExceptionThrift, ServiceHavingBeenShutdownThrift,
      ServiceIsNotAvailableThrift, TException {

    //check the instance status
    checkInstanceStatus("cancelVolAccessRuleAllApplied");
    logger.warn("cancelVolumeAccessRules request: {}", request);

    securityManager.hasPermission(request.getAccountId(), "cancelVolAccessRuleAllApplied");
   

    CancelVolAccessRuleAllAppliedResponse response = new CancelVolAccessRuleAllAppliedResponse(
        request.getRequestId());
    if (request.getVolumeIds() == null || request.getVolumeIdsSize() == 0) {
      logger.warn("given volume is null");
      return response;
    }

    long toCancelRuleId = request.getRuleId();
    AccessRuleInformation accessRuleInformation = accessRuleStore.get(toCancelRuleId);
    if (accessRuleInformation == null) {
      logger.error("get access rule failed");
      throw new AccessRuleNotFoundThrift();
    }
    VolumeAccessRule volumeAccessRule = new VolumeAccessRule(accessRuleInformation);
    // unable cancel deleting access rule from volume
    if (volumeAccessRule.getStatus() == AccessRuleStatus.DELETING) {
      logger.error("The access rule {} is deleting now, unable to cancel", volumeAccessRule);
      throw new AccessRuleUnderOperationThrift();
    }

    // turn relationship info got from db to relationship structure
    List<VolumeRuleRelationshipInformation> relationshipInfoList = volumeRuleRelationshipStore
        .getByRuleId(toCancelRuleId);

    for (long toAppliedVolumeId : request.getVolumeIds()) {
      Volume2AccessRuleRelationship relationship = null;
      if (relationshipInfoList != null && relationshipInfoList.size() > 0) {
        for (VolumeRuleRelationshipInformation relationshipInfo : relationshipInfoList) {
          if (relationshipInfo.getVolumeId() == toAppliedVolumeId) {
            relationship = new Volume2AccessRuleRelationship(relationshipInfo);
            break;
          }
        }
      }
      if (relationship == null) {
        logger.error("relationship is null, {} not applied", volumeAccessRule);
        throw new AccessRuleNotAppliedThrift();
      }

      // turn status of relationship to proper status after action "cancel"
      switch (relationship.getStatus()) {
        case FREE:
          // do nothing
          break;
        case APPLING:
          VolumeAccessRuleThrift accessRuleToRemote = RequestResponseHelper
              .buildVolumeAccessRuleThriftFrom(volumeAccessRule);
          accessRuleToRemote
              .setStatus(AccessRuleStatusThrift.valueOf(relationship.getStatus().name()));
          response.addToAirVolumeIds(toAppliedVolumeId);
          break;
        case APPLIED:
          if (request.isCommit()) {
            volumeRuleRelationshipStore
                .deleteByRuleIdandVolumeId(toAppliedVolumeId, toCancelRuleId);
          } else {
            relationship.setStatus(AccessRuleStatusBindingVolume.CANCELING);
            volumeRuleRelationshipStore.save(relationship.toVolumeRuleRelationshipInformation());
          }
          break;
        case CANCELING:
          if (request.isCommit()) {
            volumeRuleRelationshipStore
                .deleteByRuleIdandVolumeId(toAppliedVolumeId, toCancelRuleId);
          }
          break;
        default:
          logger.warn("unknown status {} of relationship", relationship.getStatus());
          break;
      }
    }

    logger.warn("cancelVolumeAccessRuleAllApplied response: {}", response);
    return response;
  }

  /**
   * This is an implementation of interface. to cancel volume access rules from specified volume.
   * This is an atomic transaction.
   */
  @Override
  public CancelVolumeAccessRulesResponse cancelVolumeAccessRules(
      CancelVolumeAccessRulesRequest request)
      throws AccessRuleNotAppliedThrift, PermissionNotGrantExceptionThrift,
      ServiceHavingBeenShutdownThrift,
      ServiceIsNotAvailableThrift, AccountNotFoundExceptionThrift, AccessDeniedExceptionThrift,
      VolumeNotFoundExceptionThrift, InvalidInputExceptionThrift, TException {

    //check the instance status
    checkInstanceStatus("cancelVolumeAccessRules");
    logger.warn("cancelVolumeAccessRules request: {}", request);

    long volumeId = request.getVolumeId();
    securityManager.hasPermission(request.getAccountId(), "cancelVolumeAccessRules");
    securityManager.hasRightToAccess(request.getAccountId(), volumeId);

    CancelVolumeAccessRulesResponse cancelVolumeAccessRulesResponse;
    request.setCommit(false);
    logger.warn("cancelVolumeAccessRules request: {}", request);
    cancelVolumeAccessRulesResponse = beginCancelVolumeAccessRule(request);

    if (cancelVolumeAccessRulesResponse.getAirAccessRuleList() != null
        && !cancelVolumeAccessRulesResponse
        .getAirAccessRuleList().isEmpty()) {
      logger.warn(
          "Unable to cancel access rules in request, due to exist some other operation on them {}",
          cancelVolumeAccessRulesResponse.getAirAccessRuleList());
      logger.warn("cancelVolumeAccessRules response: {}", cancelVolumeAccessRulesResponse);
      return cancelVolumeAccessRulesResponse;
    }

    // get drivers binding to the volume and check if the driver is iscsi,
    // if not, finish this apply action
    VolumeMetadata volume;
    String volumeName;
    List<DriverMetadata> driverList;
    try {
      VolumeMetadataAndDrivers volumeAndDriverInfo = volumeInformationManger
          .getDriverContainVolumes(volumeId, request.getAccountId(), false);

      volume = volumeAndDriverInfo.getVolumeMetadata();
      volumeName = volume.getName();
      driverList = volumeAndDriverInfo.getDriverMetadatas();
    } catch (VolumeNotFoundException e) {
      logger.error("when cancelVolumeAccessRules, can not find the volume :{}",
          request.getVolumeId());
      throw new VolumeNotFoundExceptionThrift();
    }

    if (driverList.isEmpty() || driverList.get(0).getDriverType() != DriverType.ISCSI) {
      request.setCommit(true);
      logger.warn("cancelVolumeAccessRules request: {}", request);
      cancelVolumeAccessRulesResponse = beginCancelVolumeAccessRule(request);
      ListVolumeAccessRulesResponse response = listVolumeAccessRules(
          new ListVolumeAccessRulesRequest(request.getRequestId(), request.getAccountId()));

      List<String> hostnameList = new ArrayList<>();
      for (VolumeAccessRuleThrift volumeAccessRuleThrift : response.getAccessRules()) {
        if (request.getRuleIds().contains(volumeAccessRuleThrift.getRuleId())) {
          hostnameList.add(volumeAccessRuleThrift.getIncomingHostName());
        }
      }
      buildEndOperationWrtCancelVolumeAccessRulesAndSaveToDb(request.getAccountId(),
          request.getVolumeId(),
          volumeName, hostnameList.stream().collect(Collectors.joining(", ")));
      logger.warn("cancelVolumeAccessRules response: {}", cancelVolumeAccessRulesResponse);
      return cancelVolumeAccessRulesResponse;
    }

    request.setCommit(true);
    logger.warn("cancelVolumeAccessRules request: {}", request);
    cancelVolumeAccessRulesResponse = beginCancelVolumeAccessRule(request);

    ListVolumeAccessRulesRequest listVolumeAccessRulesRequest = new ListVolumeAccessRulesRequest(
        request.getRequestId(), request.getAccountId());

    ListVolumeAccessRulesResponse response;
    try {
      response = listVolumeAccessRules(listVolumeAccessRulesRequest);
    } catch (TException e) {
      logger.error("Caught an exception", e);
      throw e;
    } catch (Exception e) {
      logger.error("Caught an exception", e);
      throw new TException(e);
    }

    List<String> hostnameList = new ArrayList<>();
    for (VolumeAccessRuleThrift volumeAccessRuleThrift : response.getAccessRules()) {
      if (request.getRuleIds().contains(volumeAccessRuleThrift.getRuleId())) {
        hostnameList.add(volumeAccessRuleThrift.getIncomingHostName());
      }
    }
    buildEndOperationWrtCancelVolumeAccessRulesAndSaveToDb(request.getAccountId(),
        request.getVolumeId(),
        volumeName, hostnameList.stream().collect(Collectors.joining(", ")));

    logger.warn("cancelVolumeAccessRules response: {}", cancelVolumeAccessRulesResponse);
    return cancelVolumeAccessRulesResponse;
  }


  public CancelVolumeAccessRulesResponse beginCancelVolumeAccessRule(
      CancelVolumeAccessRulesRequest request)
      throws AccessRuleNotAppliedThrift {

    CancelVolumeAccessRulesResponse response = new CancelVolumeAccessRulesResponse(
        request.getRequestId());
    if (request.getRuleIds() == null || request.getRuleIdsSize() == 0) {
      return response;
    }

    for (long toCancelRuleId : request.getRuleIds()) {
      AccessRuleInformation accessRuleInformation = accessRuleStore.get(toCancelRuleId);
      if (accessRuleInformation == null) {
        continue;
      }

      VolumeAccessRule volumeAccessRule = new VolumeAccessRule(accessRuleInformation);
      // unable cancel deleting access rule from volume
      if (volumeAccessRule.getStatus() == AccessRuleStatus.DELETING) {
        logger.warn("The access rule {} is deleting now, unable to cancel this rule from volume {}",
            volumeAccessRule, request.getVolumeId());
        response.addToAirAccessRuleList(
            RequestResponseHelper.buildVolumeAccessRuleThriftFrom(volumeAccessRule));
        continue;
      }

      // turn relationship info got from db to relationship structure
      List<VolumeRuleRelationshipInformation> relationshipInfoList = volumeRuleRelationshipStore
          .getByRuleId(toCancelRuleId);
      Volume2AccessRuleRelationship relationship = null;
      if (relationshipInfoList != null && relationshipInfoList.size() > 0) {
        for (VolumeRuleRelationshipInformation relationshipInfo : relationshipInfoList) {
          if (relationshipInfo.getVolumeId() == request.getVolumeId()) {
            relationship = new Volume2AccessRuleRelationship(relationshipInfo);
            break;
          }
        }
      }
      if (relationship == null) {
        logger.error("The access rule {} is deleting now, unable to cancel", volumeAccessRule);
        throw new AccessRuleNotAppliedThrift();
      }

      // turn status of relationship to proper status after action "cancel"
      switch (relationship.getStatus()) {
        case FREE:
          // do nothing
          break;
        case APPLING:
          VolumeAccessRuleThrift accessRuleToRemote = RequestResponseHelper
              .buildVolumeAccessRuleThriftFrom(volumeAccessRule);
          accessRuleToRemote
              .setStatus(AccessRuleStatusThrift.valueOf(relationship.getStatus().name()));
          response.addToAirAccessRuleList(accessRuleToRemote);
          break;
        case APPLIED:
          if (request.isCommit()) {
            volumeRuleRelationshipStore
                .deleteByRuleIdandVolumeId(request.getVolumeId(), toCancelRuleId);
          } else {
            relationship.setStatus(AccessRuleStatusBindingVolume.CANCELING);
            volumeRuleRelationshipStore.save(relationship.toVolumeRuleRelationshipInformation());
          }
          break;
        case CANCELING:
          if (request.isCommit()) {
            volumeRuleRelationshipStore
                .deleteByRuleIdandVolumeId(request.getVolumeId(), toCancelRuleId);
          }
          break;
        default:
          logger.warn("unknown status {} of relationship", relationship.getStatus());
          break;
      }
    }

    logger.warn("beginCancelVolumeAccessRule response: {}", response);
    return response;
  }

  /**
   * This method is an implementation of interface to apply access rules to specified volumes.
   */
  @Override
  public ApplyVolumeAccessRuleOnVolumesResponse applyVolumeAccessRuleOnVolumes(
      ApplyVolumeAccessRuleOnVolumesRequest request)
      throws FailedToTellDriverAboutAccessRulesExceptionThrift, VolumeNotFoundExceptionThrift,
      VolumeBeingDeletedExceptionThrift, ApplyFailedDueToVolumeIsReadOnlyExceptionThrift,
      PermissionNotGrantExceptionThrift, AccessRuleUnderOperationThrift, AccessRuleNotFoundThrift,
      AccountNotFoundExceptionThrift, ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      TException {
    //check the instance status
    checkInstanceStatus("applyVolumeAccessRuleOnVolumes");
    logger.warn("applyVolumeAccessRuleOnVolumes request: {}", request);

    securityManager.hasPermission(request.getAccountId(), "applyVolumeAccessRuleOnVolumes");
    //        securityManager.hasRightToAccess(request.getAccountId(), request.getVolumeId());

    ApplyVolumeAccessRuleOnVolumesResponse response = new ApplyVolumeAccessRuleOnVolumesResponse(
        request.getRequestId());
    response.setAirVolumeList(new ArrayList<>());
    if (request.getVolumeIds() == null || request.getVolumeIdsSize() == 0) {
      logger.warn("given volume is null");
      return response;
    }
    long toApplyRuleId = request.getRuleId();
    AccessRuleInformation accessRuleInformation = accessRuleStore.get(toApplyRuleId);
    if (accessRuleInformation == null) {
      logger.error("get access rule failed");
      throw new AccessRuleNotFoundThrift();
    }
    VolumeAccessRule volumeAccessRule = new VolumeAccessRule(accessRuleInformation);
    // unable cancel deleting access rule from volume
    if (volumeAccessRule.getStatus() == AccessRuleStatus.DELETING) {
      logger.error("The access rule {} is deleting now, unable to cancel", volumeAccessRule);
      throw new AccessRuleUnderOperationThrift();
    }

    boolean needThrowException = false;
    String exceptionDetail = new String();
    // turn relationship info get from db to relationship structure
    List<Volume2AccessRuleRelationship> relationshipList = new ArrayList<>();
    List<VolumeRuleRelationshipInformation> relationshipInfoList = volumeRuleRelationshipStore
        .getByRuleId(toApplyRuleId);
    if (relationshipInfoList != null && relationshipInfoList.size() > 0) {
      for (VolumeRuleRelationshipInformation relationshipInfo : relationshipInfoList) {
        Volume2AccessRuleRelationship relationship = new Volume2AccessRuleRelationship(
            relationshipInfo);
        relationshipList.add(relationship);
      }
    }

    logger.debug("relationshipList {} ", relationshipList);
    
    for (long toAppliedVolumeId : request.getVolumeIds()) {
      // check volume does exists or in deleting, deleted or dead status
      VolumeMetadata volume = volumeStore.getVolume(toAppliedVolumeId);
      if (volume == null) {
        logger.error("the volume {} get from volumeStore is null", toAppliedVolumeId);
        volume.setVolumeId(toAppliedVolumeId);
        response.addToAirVolumeList(RequestResponseHelper.buildThriftVolumeFrom(volume, false));
        continue;
      }
      if (volume.isDeletedByUser()) {
        logger.error("the volume {} is being deleted", toAppliedVolumeId);
        response.addToAirVolumeList(RequestResponseHelper.buildThriftVolumeFrom(volume, false));
        continue;
      }
      if (volume.getReadWrite().equals(VolumeMetadata.ReadWriteType.READONLY) && !volumeAccessRule
          .getPermission().equals(AccessPermissionType.READ)) {
        needThrowException = true;
        exceptionDetail += "Failed: " + volumeAccessRule + "\n";
        response.addToAirVolumeList(RequestResponseHelper.buildThriftVolumeFrom(volume, false));
        continue;
      }

      //diffrent access rules that witch has same ip, cannot apply to volume
      List<VolumeRuleRelationshipInformation> volumeRelationshipInfoList =
          volumeRuleRelationshipStore
              .getByVolumeId(toAppliedVolumeId);
      if (volumeRelationshipInfoList != null) {
        boolean hasSameIpRule = false;
        for (VolumeRuleRelationshipInformation relationshipInfo : volumeRelationshipInfoList) {
          AccessRuleInformation volumeAccessRuleInfo = accessRuleStore
              .get(relationshipInfo.getRuleId());

          //same ip rule already be applied with this volume
          if (accessRuleInformation.getIpAddress().equals(volumeAccessRuleInfo.getIpAddress())) {
            logger.warn(
                "apply rule:{} failed. same ip:{} rules already be applied with this volume:{}",
                toApplyRuleId, volumeAccessRuleInfo.getIpAddress(), toAppliedVolumeId);
            response.addToAirVolumeList(RequestResponseHelper.buildThriftVolumeFrom(volume, false));
            hasSameIpRule = true;
            break;
          }
        }
        if (hasSameIpRule) {
          continue;
        }
      }

      boolean existInRelationshipStore = false;
      Volume2AccessRuleRelationship relationship2Apply = new Volume2AccessRuleRelationship();
      for (Volume2AccessRuleRelationship relationship : relationshipList) {
        if (relationship.getVolumeId() == toAppliedVolumeId) {
          existInRelationshipStore = true;
          relationship2Apply = relationship;
        }
      }
      if (!existInRelationshipStore) {
        logger.warn("The rule {} is not being applied to the volume {} before", volumeAccessRule,
            toAppliedVolumeId);
        relationship2Apply = new Volume2AccessRuleRelationship();
        relationship2Apply.setRelationshipId(RequestIdBuilder.get());
        relationship2Apply.setRuleId(toApplyRuleId);
        relationship2Apply.setVolumeId(toAppliedVolumeId);
        relationship2Apply.setStatus(AccessRuleStatusBindingVolume.FREE);
        relationshipList.add(relationship2Apply);
      }

      //Turn all relationship to a proper status base on state machine below.
      switch (relationship2Apply.getStatus()) {
        case FREE:
          if (request.isCommit()) {
            relationship2Apply.setStatus(AccessRuleStatusBindingVolume.APPLIED);
          } else {
            relationship2Apply.setStatus(AccessRuleStatusBindingVolume.APPLING);
          }
          // status of relationship has changed, save it to db
          logger.debug("FREE, save {}", relationship2Apply.getStatus());
          volumeRuleRelationshipStore
              .save(relationship2Apply.toVolumeRuleRelationshipInformation());
          break;
        case APPLING:
          if (request.isCommit()) {
            relationship2Apply.setStatus(AccessRuleStatusBindingVolume.APPLIED);
            logger.debug("APPLYING , save {}", relationship2Apply.getStatus());
            volumeRuleRelationshipStore
                .save(relationship2Apply.toVolumeRuleRelationshipInformation());
          }
          break;
        case APPLIED:
          logger.debug("APPLIED, DO NOTHING");
          // do nothing
          break;
        case CANCELING:
          logger.debug("CANCELING, ERROR");
          VolumeAccessRuleThrift accessRuleToRemote = RequestResponseHelper
              .buildVolumeAccessRuleThriftFrom(volumeAccessRule);
          accessRuleToRemote
              .setStatus(AccessRuleStatusThrift.valueOf(relationship2Apply.getStatus().name()));

          response.addToAirVolumeList(RequestResponseHelper.buildThriftVolumeFrom(volume, false));
          break;
        default:
          logger.warn("Unknown status {}", relationship2Apply.getStatus());
          break;
      }
    }

    if (needThrowException) {
      ApplyFailedDueToVolumeIsReadOnlyExceptionThrift exception =
          new ApplyFailedDueToVolumeIsReadOnlyExceptionThrift();
      exception.setDetail(exceptionDetail);
      logger.warn("{}", exception);
      throw exception;
    }

    logger.warn("applyVolumeAccessRuleOnVolumes response: {}", response);
    return response;
  }
  /* VolumeAccessRules begin    *************/

  /* Iscsis info begin    *************/

  /**
   * An implemenion of interface
 * {@link InformationCenter.Iface#listVolumeAccessRules(ListVolumeAccessRulesRequest)}
   * which list all created access rules.
   */
  @Override
  public ListVolumeAccessRulesResponse listVolumeAccessRules(ListVolumeAccessRulesRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {

    //check the instance status
    checkInstanceStatus("listVolumeAccessRules");
    logger.warn("listVolumeAccessRules request: {}", request);

    ListVolumeAccessRulesResponse listVolumeAccessRulesResponse =
        new ListVolumeAccessRulesResponse();
    listVolumeAccessRulesResponse.setRequestId(request.getRequestId());

    List<AccessRuleInformation> accessRuleInformations = accessRuleStore.list();
    if (accessRuleInformations != null && accessRuleInformations.size() > 0) {
      for (AccessRuleInformation accessRuleInformation : accessRuleInformations) {
        listVolumeAccessRulesResponse.addToAccessRules(RequestResponseHelper
            .buildVolumeAccessRuleThriftFrom(new VolumeAccessRule(accessRuleInformation)));
      }
    }

    //set the List not null
    if (!listVolumeAccessRulesResponse.isSetAccessRules()) {
      listVolumeAccessRulesResponse.setAccessRules(new ArrayList<>());
    }

    logger.warn("listVolumeAccessRules response: {}", listVolumeAccessRulesResponse);
    return listVolumeAccessRulesResponse;
  }

  private List<VolumeAccessRuleThrift> listVolumeAccessRules(long accountId) {
    ListVolumeAccessRulesRequest listVolumeAccessRulesRequest = new ListVolumeAccessRulesRequest(
        RequestIdBuilder.get(), accountId);

    ListVolumeAccessRulesResponse listVolumeAccessRulesResponse = null;
    try {
      listVolumeAccessRulesResponse = listVolumeAccessRules(listVolumeAccessRulesRequest);
    } catch (TException e) {
      logger.error("in listVolumeAccessRules, get an exception :{}", e);
    }

    List<VolumeAccessRuleThrift> volumeAccessRuleList = new ArrayList<>();
    if (listVolumeAccessRulesResponse != null
        && listVolumeAccessRulesResponse.getAccessRules() != null
        && listVolumeAccessRulesResponse.getAccessRulesSize() > 0) {
      volumeAccessRuleList.addAll(listVolumeAccessRulesResponse.getAccessRules());
    }

    logger.info("in listVolumeAccessRules, get volumeAccessRuleList :{}", volumeAccessRuleList);
    return volumeAccessRuleList;
  }

  @Override
  public ListVolumeAccessRulesByVolumeIdsResponse listVolumeAccessRulesByVolumeIds(
      ListVolumeAccessRulesByVolumeIdsRequest request)
      throws ServiceIsNotAvailableThrift, TException {
    //check the instance status
    checkInstanceStatus("listVolumeAccessRulesByVolumeIds");
    logger.warn("listVolumeAccessRulesByVolumeIds request: {}", request);

    ListVolumeAccessRulesByVolumeIdsResponse listVolumeAccessRulesByVolumeIdsResponse =
        new ListVolumeAccessRulesByVolumeIdsResponse();
    listVolumeAccessRulesByVolumeIdsResponse.setRequestId(request.getRequestId());
    Set<Long> volumeIds = request.getVolumeIds();

    List<VolumeRuleRelationshipInformation> volumeRuleRelationshipInformationList =
        volumeRuleRelationshipStore
            .list();
    List<AccessRuleInformation> accessRuleInformations = accessRuleStore.list();

    for (Long volumeId : volumeIds) {
      VolumeMetadata volumeMetadata = null;
      try {
        volumeMetadata = volumeInformationManger
            .getVolumeNew(volumeId, Constants.SUPERADMIN_ACCOUNT_ID);
      } catch (VolumeNotFoundException e) {
        logger.warn("when listVolumeAccessRulesByVolumeIds, can not find the volume :{}", volumeId);
        continue;
      }

      if (allDriverClearAccessRulesManager.isVolumeNeedClearAccessRules(volumeId)) {
        continue;
      }

      List<VolumeAccessRuleThrift> accessRuleThriftList = new ArrayList<>();
      for (VolumeRuleRelationshipInformation volumeRule : volumeRuleRelationshipInformationList) {
        if (volumeId == volumeRule.getVolumeId()) {
          Long ruleId = volumeRule.getRuleId();
          for (AccessRuleInformation rule : accessRuleInformations) {
            if (ruleId == rule.getRuleId()) {
              accessRuleThriftList.add(RequestResponseHelper
                  .buildVolumeAccessRuleThriftFrom(new VolumeAccessRule(rule)));
            }
          }
        }
      }
      listVolumeAccessRulesByVolumeIdsResponse
          .putToAccessRulesTable(volumeId, accessRuleThriftList);
    }

    logger.warn("listVolumeAccessRulesByVolumeIds response: {}",
        listVolumeAccessRulesByVolumeIdsResponse);
    return listVolumeAccessRulesByVolumeIdsResponse;
  }

  /**
   * An implementation of interface.
 * {@link InformationCenter.Iface#createIscsiAccessRules(CreateIscsiAccessRulesRequest)}
   * which create an specified access rule in request to database.
   *
   * <p>Status of a new access rule is always "available".
   */
  @Override
  public synchronized CreateIscsiAccessRulesResponse createIscsiAccessRules(
      CreateIscsiAccessRulesRequest request)
      throws IscsiAccessRuleDuplicateThrift, IscsiAccessRuleFormatErrorThrift,
      InvalidInputExceptionThrift,
      ServiceHavingBeenShutdownThrift, ChapSameUserPasswdErrorThrift, ServiceIsNotAvailableThrift,
      AccountNotFoundExceptionThrift, PermissionNotGrantExceptionThrift, TException {

    //check the instance status
    checkInstanceStatus("createIscsiAccessRules");
    logger.warn("creatIscsiAccessRules request: {}", request);
    securityManager.hasPermission(request.getAccountId(), "createIscsiAccessRules");

    CreateIscsiAccessRulesResponse response = new CreateIscsiAccessRulesResponse(
        request.getRequestId());
    if (request.getAccessRules() == null || request.getAccessRulesSize() == 0) {
      logger.debug("No access rule to create in request, nothing to do");
      return response;
    }
    //check initiatorname format
    for (IscsiAccessRuleThrift iscsiAccessRuleThrift : request.getAccessRules()) {
      boolean b = iscsiAccessRuleThrift.getInitiatorName().matches("iqn.*");
      if (!b) {
        logger.error("Initiator Name format fail");
        throw new IscsiAccessRuleFormatErrorThrift();
      }
    }
    // check user of incoming and outgoing are not the same
    // when outgoing set incoming should set first
    for (IscsiAccessRuleThrift iscsiAccessRuleThrift : request.getAccessRules()) {
      String user = iscsiAccessRuleThrift.getUser();
      String outUser = iscsiAccessRuleThrift.getOutUser();
      if ((user != null && user != "") && (outUser != null && outUser != "")) {
        if (user.equals(outUser)) {
          logger.error("incoming and outgoing user should not the same");
          throw new ChapSameUserPasswdErrorThrift();
        }
      }
      if ((user == null || user == "") && (outUser != null && outUser != "")) {
        logger.error("incoming user should set first");
        throw new InvalidInputExceptionThrift();
      }
    }
    // checkout each of the rule that were stored in the request to find out
    // whether there are some rules duplicated
    for (IscsiAccessRuleThrift iscsiAccessRuleThrift1 : request.getAccessRules()) {
      int counter = 0;
      for (IscsiAccessRuleThrift iscsiAccessRuleThrift2 : request.getAccessRules()) {
        if (iscsiAccessRuleThrift1.equals(iscsiAccessRuleThrift2)) {
          counter++;
        }
      }
      if (counter > 1) {
        throw new InvalidInputExceptionThrift();
      }
    }
    // get iscsi access rule list from iscsiAccessRuleStore
    List<IscsiAccessRuleInformation> iscsiRulesFromDb = iscsiAccessRuleStore.list();
   
   
   
    for (IscsiAccessRuleThrift iscsiAccessRuleThrift : request.getAccessRules()) {
      boolean accessRuleExisted = false;
      for (IscsiAccessRuleInformation rule : iscsiRulesFromDb) {
        if (rule.getInitiatorName().equals(iscsiAccessRuleThrift.getInitiatorName()) && rule
            .getUser()
            .equals(iscsiAccessRuleThrift.getUser()) && rule.getPassed()
            .equals(iscsiAccessRuleThrift.getPassed())
            && rule.getPermission() == iscsiAccessRuleThrift
            .getPermission().getValue()) {
          accessRuleExisted = true;
          break;
        } else {
          // do nothing
        }
      }
      if (!accessRuleExisted) {
        logger.debug("information center going to save the access rules");
        IscsiAccessRule iscsiAccessRule = RequestResponseHelper
            .buildIscsiAccessRuleFrom(iscsiAccessRuleThrift);
        // status "available" for a new access rule
        iscsiAccessRule.setStatus(AccessRuleStatus.AVAILABLE);
        iscsiAccessRuleStore.save(iscsiAccessRule.toIscsiAccessRuleInformation());
      } else {
        // throw an exception out
        logger.debug("information center throws an exception IscsiAccessRuleDuplicateThrift");
        throw new IscsiAccessRuleDuplicateThrift();
      }
    }

    String initiatorName = request.getAccessRules().get(0).getInitiatorName();
    buildEndOperationWrtCreateVolumeAccessRulesAndSaveToDb(request.getAccountId(), initiatorName);
    logger.warn("createIscsiAccessRules response: {}", response);
    return response;
  }

  /**
   * An implementation of interface
   * {@link ControlCenter.Iface#deleteIscsiAccessRules(DeleteIscsiAccessRulesRequest)}
   * to delete iscsi access rules. This is an atomic transaction which contains coordination between
   * driver container and infocenter. Node control center plays a role as coordinator.
   */
  @Override
  public DeleteIscsiAccessRulesResponse deleteIscsiAccessRules(
      DeleteIscsiAccessRulesRequest request)
      throws ServiceHavingBeenShutdownThrift, FailedToTellDriverAboutAccessRulesExceptionThrift,
      ServiceIsNotAvailableThrift, AccountNotFoundExceptionThrift,
      PermissionNotGrantExceptionThrift,
      TException {

    //check the instance status
    checkInstanceStatus("deleteIscsiAccessRules");
    logger.warn("deleteIscsiAccessRules request: {}", request);

    securityManager.hasPermission(request.getAccountId(), "deleteIscsiAccessRules");

    ListIscsiAccessRulesResponse listIscsiAccessRulesResponse;
    try {
      listIscsiAccessRulesResponse = listIscsiAccessRules(
          new ListIscsiAccessRulesRequest(request.getRequestId(), request.getAccountId()));
    } catch (TException e) {
      logger.error("Caught an exception", e);
      throw e;
    } catch (Exception e) {
      logger.error("Caught an exception", e);
      throw new TException(e);
    }

    List<String> hostnameList = new ArrayList<>();
    if (listIscsiAccessRulesResponse.getAccessRules() != null) {
      for (IscsiAccessRuleThrift iscsiAccessRuleThrift : listIscsiAccessRulesResponse
          .getAccessRules()) {
        if (request.getRuleIds().contains(iscsiAccessRuleThrift.getRuleId())) {
          hostnameList.add(iscsiAccessRuleThrift.getInitiatorName());
        }
      }
    }

    DeleteIscsiAccessRulesResponse deleteIscsiAccessRulesResponse;
    try {
      deleteIscsiAccessRulesResponse = beginDeleteIscsiAccessRule(request);
    } catch (TException e) {
      logger.error("Caught an exception", e);
      throw e;
    } catch (Exception e) {
      logger.error("Caught an exception", e);
      throw new TException(e);
    }
    logger.warn("deleteIscsiAccessRules response: {}", deleteIscsiAccessRulesResponse);

    if (deleteIscsiAccessRulesResponse.getAirAccessRuleList() != null
        && !deleteIscsiAccessRulesResponse
        .getAirAccessRuleList().isEmpty()) {
      logger.warn(
          "Unable to delete access rules in request, due to exist air access rules {} which are "
              + "operating by other action",
          deleteIscsiAccessRulesResponse.getAirAccessRuleList());
      logger.warn("deleteIscsiAccessRules response: {}", deleteIscsiAccessRulesResponse);
      return deleteIscsiAccessRulesResponse;
    }

    buildEndOperationWrtDeleteVolumeAccessRulesAndSaveToDb(request.getAccountId(),
        hostnameList.stream().collect(Collectors.joining(", ")));

    return deleteIscsiAccessRulesResponse;
  }


  public DeleteIscsiAccessRulesResponse beginDeleteIscsiAccessRule(
      DeleteIscsiAccessRulesRequest request)
      throws ServiceIsNotAvailableThrift, ServiceHavingBeenShutdownThrift, TException {

    // prepare a response for deleting iscsi access rule request
    DeleteIscsiAccessRulesResponse response = new DeleteIscsiAccessRulesResponse(
        request.getRequestId());
    if (request.getRuleIds() == null || request.getRuleIds().size() == 0) {
      logger.debug("No iscsi access rules existing in request to delete");
      return response;
    }
    for (long ruleId : request.getRuleIds()) {
      IscsiAccessRuleInformation iscsiAccessRuleInformation = iscsiAccessRuleStore.get(ruleId);
      if (iscsiAccessRuleInformation == null) {
        logger.debug("No iscsi access rule with id {}", ruleId);
        continue;
      }
      IscsiAccessRule iscsiAccessRule = new IscsiAccessRule(iscsiAccessRuleInformation);

      List<Iscsi2AccessRuleRelationship> iscsiRelationshipList =
          new ArrayList<Iscsi2AccessRuleRelationship>();
      List<IscsiRuleRelationshipInformation> iscsiRelationshipInfoList = iscsiRuleRelationshipStore
          .getByRuleId(ruleId);
      if (iscsiRelationshipInfoList != null && iscsiRelationshipInfoList.size() > 0) {
        for (IscsiRuleRelationshipInformation relationshipInfo : iscsiRelationshipInfoList) {
          iscsiRelationshipList.add(new Iscsi2AccessRuleRelationship(relationshipInfo));
        }
      } else {
        logger.debug("Rule {} is not applied before", iscsiAccessRule);
      }
      // a flag used to check if the access rule is able to be deleted
      boolean existAirAccessRule = false;
      for (Iscsi2AccessRuleRelationship relationship : iscsiRelationshipList) {
        
        switch (relationship.getStatus()) {
          case FREE:
            // do nothing
            break;
          case APPLIED:
            // do nothing
            break;
          case APPLING:
          case CANCELING:
            existAirAccessRule = true;
            break;
          default:
            logger.warn("Unknown status {} of relationship", relationship.getStatus());
            break;
        }

        if (existAirAccessRule) {
          // unable to delete the access rule, so just break
          break;
        }
      }
      if (existAirAccessRule) {
        logger.debug("Iscsi Access rule {} already has an operation on it before deleting",
            iscsiAccessRule);
        response.addToAirAccessRuleList(
            RequestResponseHelper.buildIscsiAccessRuleThriftFrom(iscsiAccessRule));
        continue;
      }
      if (!request.isCommit()) {
        if (iscsiAccessRule.getStatus() != AccessRuleStatus.DELETING) {
          iscsiAccessRule.setStatus(AccessRuleStatus.DELETING);
          iscsiAccessRuleStore.save(iscsiAccessRule.toIscsiAccessRuleInformation());
        } else {
          // do nothing
        }
        continue;
      }

      // delete the iscsi access rule and relationship with driverKey from
      // database if exists
      iscsiAccessRuleStore.delete(ruleId);
      iscsiRuleRelationshipStore.deleteByRuleId(ruleId);
    }
    logger.warn("deleteIscsiAccessRules response: {}", response);
    return response;
  }

  /**
   * An implementation of interface
   * {@link InformationCenter.Iface#getIscsiAccessRules(GetIscsiAccessRulesRequest)}
   * which get the specified access rules applied to the iscsi in request.
   */
  @Override
  public GetIscsiAccessRulesResponse getIscsiAccessRules(GetIscsiAccessRulesRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {
    //check the instance status
    checkInstanceStatus("getIscsiAccessRules");
    logger.warn("getIscsiAccessRules request: {}", request);
    if (!backupDbManager.passedRecoveryTime()) {
      logger.warn(
          "backup database not done, just wait its working done and we can get real access rules");
      throw new ServiceIsNotAvailableThrift();
    }

    // prepare a response to getting iscsi access rules request
    GetIscsiAccessRulesResponse response = new GetIscsiAccessRulesResponse();
    response.setRequestId(request.getRequestId());

    List<IscsiRuleRelationshipInformation> relationshipInfoList = iscsiRuleRelationshipStore
        .getByDriverKey(RequestResponseHelper.buildDriverKeyFrom(request.getDriverKey()));
    if (relationshipInfoList == null || relationshipInfoList.size() == 0) {
      logger.debug("The driver {} hasn't be applied any access rules", request.getDriverKey());
      return response;
    }

    List<Iscsi2AccessRuleRelationship> relationshipList =
        new ArrayList<Iscsi2AccessRuleRelationship>();
    for (IscsiRuleRelationshipInformation relationshipInfo : relationshipInfoList) {
      Iscsi2AccessRuleRelationship relationship = new Iscsi2AccessRuleRelationship(
          relationshipInfo);
      relationshipList.add(relationship);
    }

    for (Iscsi2AccessRuleRelationship relationship : relationshipList) {
      long volumeId = relationship.getVolumeId();

      VolumeMetadata volumeMetadata = null;
      try {
        volumeMetadata = volumeInformationManger.getVolumeNew(volumeId, request.getAccountId());
      } catch (VolumeNotFoundException e) {
        logger.warn("when getIscsiAccessRules, can not find the volume :{}", volumeId);
        continue;
      }

      if (volumeIdToSegmentWrapperCountMap.get(volumeId) != null) {
        continue;
      }
      IscsiAccessRule iscsiAccessRule = new IscsiAccessRule(
          iscsiAccessRuleStore.get(relationship.getRuleId()));
      // build access rule to remote
      IscsiAccessRuleThrift iscsiAccessRuleToRemote = RequestResponseHelper
          .buildIscsiAccessRuleThriftFrom(iscsiAccessRule);
      iscsiAccessRuleToRemote
          .setStatus(AccessRuleStatusThrift.valueOf(relationship.getStatus().name()));
      response.addToAccessRules(iscsiAccessRuleToRemote);
    }

    logger.warn("getIscsiAccessRules response: {}", response);
    return response;
  }

  @Override
  public ReportIscsiAccessRulesResponse reportIscsiAccessRules(
      ReportIscsiAccessRulesRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {
    checkInstanceStatus("reportIscsiAccessRules");
    logger.warn("reportIscsiAccessRules request: {}", request);

    // prepare a response to getting iscsi access rules request
    ReportIscsiAccessRulesResponse response = new ReportIscsiAccessRulesResponse();
    response.setRequestId(request.getRequestId());

    DriverKey driverKey = RequestResponseHelper.buildDriverKeyFrom(request.getDriverKey());

    if (Objects.isNull(request.getAccessRules()) || request.getAccessRules().size() == 0) {
      logger.info("driver {} rules is null", driverKey.getDriverContainerId());
      allDriverClearAccessRulesManager
          .addDriverHasClearAccessRules(driverKey.getVolumeId(), driverKey.getDriverContainerId());
    }

    return response;
  }

  /**
   * An implementation of interface
   * {@link InformationCenter.Iface#applyIscsiAccessRules(ApplyIscsiAccessRulesRequest)}
   * which apply specified access rule to the drivers in request.
   *
   * <p>If exist some air rules which means unable to apply those rules to specified driver in
   * request, add those rules in air list of response.
   *
   */
  @Override
  public ApplyIscsiAccessRulesResponse applyIscsiAccessRules(ApplyIscsiAccessRulesRequest request)
      throws IscsiNotFoundExceptionThrift, IscsiBeingDeletedExceptionThrift,
      ServiceIsNotAvailableThrift,
      ApplyFailedDueToConflictExceptionThrift, TException {
    //check the instance status
    checkInstanceStatus("applyIscsiAccessRules");
    logger.warn("applyIscsiAccessRules request: {}", request);

    securityManager.hasPermission(request.getAccountId(), "applyIscsiAccessRules");
    securityManager.hasRightToAccess(request.getAccountId(), request.getDriverKey().getVolumeId());

    // check driver does exists or in deleting, deleted or dead status
    DriverMetadata driver;
    try {
      DriverKeyThrift driverKey = request.getDriverKey();
      driver = driverStore.get(driverKey.getDriverContainerId(), driverKey.getVolumeId(),
          DriverType.valueOf(driverKey.getDriverType().name()), driverKey.getSnapshotId());
    } catch (Exception e) {
      throw new DriverNotFoundExceptionThrift();
    }

    if (driver == null) {
      throw new DriverNotFoundExceptionThrift();
    }

    if (driver.getDriverStatus() == DriverStatus.REMOVING) {
      throw new IscsiBeingDeletedExceptionThrift();
    }

    ApplyIscsiAccessRulesResponse response = new ApplyIscsiAccessRulesResponse(
        request.getRequestId());

    List<Long> toApplyRuleIdList = request.getRuleIds();
    if (toApplyRuleIdList == null || toApplyRuleIdList.isEmpty()) {
      logger.debug("No rule exists in applying request, do nothing");
      return response;
    }

    String exceptionDetail = new String();
    for (long toApplyRuleId : toApplyRuleIdList) {
      IscsiAccessRule iscsiAccessRule = new IscsiAccessRule(
          iscsiAccessRuleStore.get(toApplyRuleId));

      // unable to apply a deleting access rule to iscsi
      if (iscsiAccessRule.getStatus() == AccessRuleStatus.DELETING) {
        logger.warn(
            "The iscsi access rule {} is deleting now, unable to apply this rule to driver {}",
            iscsiAccessRule, request.getDriverKey());
        response.addToAirAccessRuleList(
            RequestResponseHelper.buildIscsiAccessRuleThriftFrom(iscsiAccessRule));
        continue;
      }

      // turn relationship info get from db to relationship structure
      List<Iscsi2AccessRuleRelationship> relationshipList = new ArrayList<>();
      List<IscsiRuleRelationshipInformation> relationshipInfoList = iscsiRuleRelationshipStore
          .getByRuleId(toApplyRuleId);
      if (relationshipInfoList != null && relationshipInfoList.size() > 0) {
        for (IscsiRuleRelationshipInformation relationshipInfo : relationshipInfoList) {
          Iscsi2AccessRuleRelationship relationship = new Iscsi2AccessRuleRelationship(
              relationshipInfo);
          relationshipList.add(relationship);
        }
      }
      /*
       * Check if the relationship exists in db. If it doesn't, in the case, the relationship is
       * a fresh one, we
       * should set its status to "free"; Otherwise, keep the status of relationship get from db.
       */
      DriverKeyThrift driverKey = request.getDriverKey();
      boolean existInRelationshipStore = false;
      Iscsi2AccessRuleRelationship relationship2Apply = new Iscsi2AccessRuleRelationship();
      for (Iscsi2AccessRuleRelationship relationship : relationshipList) {
        if (relationship.getDriverContainerId() == driverKey.getDriverContainerId()
            && relationship.getVolumeId() == driverKey.getVolumeId()
            && relationship.getSnapshotId() == driverKey.getSnapshotId() && relationship
            .getDriverType()
            .equals(driverKey.getDriverType().name())) {
          existInRelationshipStore = true;
          relationship2Apply = relationship;
        }
      }
      if (!existInRelationshipStore) {
        logger.debug("The rule {} is not being applied to the driver {} before", iscsiAccessRule,
            request.getDriverKey());
        relationship2Apply = new Iscsi2AccessRuleRelationship();
        relationship2Apply.setRelationshipId(RequestIdBuilder.get());
        relationship2Apply.setRuleId(toApplyRuleId);
        relationship2Apply.setDriverContainerId(driverKey.getDriverContainerId());
        relationship2Apply.setDriverType(driverKey.getDriverType().name());
        relationship2Apply.setSnapshotId(driverKey.getSnapshotId());
        relationship2Apply.setVolumeId(driverKey.getVolumeId());
        relationship2Apply.setStatus(AccessRuleStatusBindingVolume.FREE);
        relationshipList.add(relationship2Apply);
      }
      /*
       * Turn all relationship to a proper status base on state machine below.
       * A state machine to transfer status of relationship to proper status.
       */
      switch (relationship2Apply.getStatus()) {
        case FREE:
          if (request.isCommit()) {
            relationship2Apply.setStatus(AccessRuleStatusBindingVolume.APPLIED);
          } else {
            relationship2Apply.setStatus(AccessRuleStatusBindingVolume.APPLING);
          }
          // status of relationship has changed, save it to db
          logger.debug("FREE, save {}", relationship2Apply.getStatus());
          iscsiRuleRelationshipStore.save(relationship2Apply.toIscsiRuleRelationshipInformation());
          break;
        case APPLING:
          if (request.isCommit()) {
            relationship2Apply.setStatus(AccessRuleStatusBindingVolume.APPLIED);
            logger.debug("APPLYING , save {}", relationship2Apply.getStatus());
            iscsiRuleRelationshipStore
                .save(relationship2Apply.toIscsiRuleRelationshipInformation());
          }
          break;
        case APPLIED:
          logger.debug("APPLIED, DO NOTHING");
          // do nothing
          break;
        case CANCELING:
          logger.debug("CANCELING, ERROR");
          IscsiAccessRuleThrift iscsiAccessRuleToRemote = RequestResponseHelper
              .buildIscsiAccessRuleThriftFrom(iscsiAccessRule);
          iscsiAccessRuleToRemote
              .setStatus(AccessRuleStatusThrift.valueOf(relationship2Apply.getStatus().name()));
          response.addToAirAccessRuleList(iscsiAccessRuleToRemote);
          break;
        default:
          logger.warn("Unknown status {}", relationship2Apply.getStatus());
          break;
      }
    }

    if (response.getAirAccessRuleList() != null && !response.getAirAccessRuleList().isEmpty()) {
      logger.warn(
          "Unable to apply access rules to specified iscsi in request, due to exist other "
              + "operation on the rules {}",
          response.getAirAccessRuleList());
      return response;
    }

    logger.warn("applyIscsiAccessRules response: {}", response);
    return response;
  }

  /**
   * An implementation of interface
   * {@link InformationCenter.Iface#cancelIscsiAccessRules(CancelIscsiAccessRulesRequest)}
   * which to cancel access rule from specified iscsi in request.
   */
  @Override
  public CancelIscsiAccessRulesResponse cancelIscsiAccessRules(
      CancelIscsiAccessRulesRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      AccessRuleNotAppliedThrift,
      TException {

    //check the instance status
    checkInstanceStatus("cancelIscsiAccessRules");
    logger.warn("cancelIscsiAccessRules request: {}", request);
    securityManager.hasPermission(request.getAccountId(), "cancelIscsiAccessRules");
    securityManager.hasRightToAccess(request.getAccountId(), request.getDriverKey().getVolumeId());

    CancelIscsiAccessRulesResponse response = new CancelIscsiAccessRulesResponse(
        request.getRequestId());

    if (request.getRuleIds() == null || request.getRuleIdsSize() == 0) {
      return response;
    }

    for (long toCancelRuleId : request.getRuleIds()) {

      IscsiAccessRuleInformation iscsiAccessRuleInformation = iscsiAccessRuleStore
          .get(toCancelRuleId);
      if (iscsiAccessRuleInformation == null) {
        continue;
      }
      IscsiAccessRule iscsiAccessRule = new IscsiAccessRule(iscsiAccessRuleInformation);
      // unable cancel deleting access rule from volume
      if (iscsiAccessRule.getStatus() == AccessRuleStatus.DELETING) {
        logger.debug(
            "The iscsi access rule {} is deleting now, unable to cancel this rule from drivers {}",
            iscsiAccessRule, request.getDriverKey());
        response.addToAirAccessRuleList(
            RequestResponseHelper.buildIscsiAccessRuleThriftFrom(iscsiAccessRule));
        continue;
      }
      // turn relationship info got from db to relationship structure
      List<IscsiRuleRelationshipInformation> relationshipInfoList = iscsiRuleRelationshipStore
          .getByRuleId(toCancelRuleId);
      Iscsi2AccessRuleRelationship relationship = null;
      if (relationshipInfoList != null && relationshipInfoList.size() > 0) {
        for (IscsiRuleRelationshipInformation relationshipInfo : relationshipInfoList) {
          DriverKeyThrift driverKey = request.getDriverKey();
          if (relationshipInfo.getDriverContainerId() == driverKey.getDriverContainerId()
              && relationshipInfo.getVolumeId() == driverKey.getVolumeId()
              && relationshipInfo.getSnapshotId() == driverKey.getSnapshotId() && relationshipInfo
              .getDriverType().equalsIgnoreCase(driverKey.getDriverType().name())) {
            relationship = new Iscsi2AccessRuleRelationship(relationshipInfo);
            break;
          }
        }
      }
      if (relationship == null) {
        throw new AccessRuleNotAppliedThrift();
      }
      
      switch (relationship.getStatus()) {
        case FREE:
          // do nothingin
          break;
        case APPLING:
          IscsiAccessRuleThrift iscsiAccessRuleToRemote = RequestResponseHelper
              .buildIscsiAccessRuleThriftFrom(iscsiAccessRule);
          iscsiAccessRuleToRemote
              .setStatus(AccessRuleStatusThrift.valueOf(relationship.getStatus().name()));
          response.addToAirAccessRuleList(iscsiAccessRuleToRemote);
          break;
        case APPLIED:
          if (request.isCommit()) {
            iscsiRuleRelationshipStore.deleteByRuleIdandDriverKey(
                RequestResponseHelper.buildDriverKeyFrom(request.getDriverKey()), toCancelRuleId);
          } else {
            relationship.setStatus(AccessRuleStatusBindingVolume.CANCELING);
            iscsiRuleRelationshipStore.save(relationship.toIscsiRuleRelationshipInformation());
          }
          break;
        case CANCELING:
          if (request.isCommit()) {
            iscsiRuleRelationshipStore.deleteByRuleIdandDriverKey(
                RequestResponseHelper.buildDriverKeyFrom(request.getDriverKey()), toCancelRuleId);
          }
          break;
        default:
          logger.warn("unknown status {} of relationship", relationship.getStatus());
          break;
      }
    }
    logger.debug("cancelIscsiAccessRules response: {}", response);
    return response;
  }

  /**
   * An implementation of interface which apply specified access rule to specified drivers in
   * request.
   */
  @Override
  public synchronized ApplyIscsiAccessRuleOnIscsisResponse applyIscsiAccessRuleOnIscsis(
      ApplyIscsiAccessRuleOnIscsisRequest request)
      throws IscsiNotFoundExceptionThrift, IscsiBeingDeletedExceptionThrift,
      ServiceIsNotAvailableThrift,
      ApplyFailedDueToConflictExceptionThrift, IscsiAccessRuleUnderOperationThrift,
      IscsiAccessRuleNotFoundThrift, TException {
    //check the instance status
    checkInstanceStatus("applyIscsiAccessRuleOnIscsis");
    logger.warn("applyIscsiAccessRuleOnIscsis request: {}", request);

    ApplyIscsiAccessRuleOnIscsisResponse response = new ApplyIscsiAccessRuleOnIscsisResponse(
        request.getRequestId());
    if (request.getDriverKeys() == null || request.getDriverKeysSize() == 0) {
      logger.error("given drive is null");
      return response;
    }

    long toApplyRuleId = request.getRuleId();
    IscsiAccessRuleInformation iscsiAccessRuleInformation = iscsiAccessRuleStore.get(toApplyRuleId);
    if (iscsiAccessRuleInformation == null) {
      logger.error("The iscsi access rule not found error");
      throw new IscsiAccessRuleNotFoundThrift();
    }
    IscsiAccessRule iscsiAccessRule = new IscsiAccessRule(iscsiAccessRuleInformation);
    // unable cancel deleting access rule from volume
    if (iscsiAccessRule.getStatus() == AccessRuleStatus.DELETING) {
      logger.error("The iscsi access rule {} is deleting now, unable to cancel", iscsiAccessRule);
      throw new IscsiAccessRuleUnderOperationThrift();
    }

    // turn relationship info get from db to relationship structure
    List<Iscsi2AccessRuleRelationship> relationshipList = new ArrayList<>();
    List<IscsiRuleRelationshipInformation> relationshipInfoList = iscsiRuleRelationshipStore
        .getByRuleId(toApplyRuleId);
    if (relationshipInfoList != null && relationshipInfoList.size() > 0) {
      for (IscsiRuleRelationshipInformation relationshipInfo : relationshipInfoList) {
        Iscsi2AccessRuleRelationship relationship = new Iscsi2AccessRuleRelationship(
            relationshipInfo);
        relationshipList.add(relationship);
      }
    }

    /*
     * Check if the relationship exists in db. If it doesn't, in the case, the relationship is a
     * fresh one, we
     * should set its status to "free"; Otherwise, keep the status of relationship get from db.
     */
    for (DriverKeyThrift driverKey : request.getDriverKeys()) {
      // check driver does exists or in deleting, deleted or dead status
      DriverMetadata driver = null;
      try {
        driver = driverStore.get(driverKey.getDriverContainerId(), driverKey.getVolumeId(),
            DriverType.valueOf(driverKey.getDriverType().name()), driverKey.getSnapshotId());
      } catch (Exception e) {
        logger.error("get driverKey exception", e);
      }
      if (driver == null) {
        //throw new DriverNotFoundExceptionThrift();
        logger.error("driverKey {} not in driverStore", driverKey);
        response.addToAirDriverKeyList(driverKey);
        continue;
      }
      if (driver.getDriverStatus() == DriverStatus.REMOVING) {
        //throw new IscsiBeingDeletedExceptionThrift();
        logger.error("driverKey {} is removing", driverKey);
        response.addToAirDriverKeyList(driverKey);
        continue;
      }

      boolean existInRelationshipStore = false;
      Iscsi2AccessRuleRelationship relationship2Apply = new Iscsi2AccessRuleRelationship();
      for (Iscsi2AccessRuleRelationship relationship : relationshipList) {
        if (relationship.getDriverContainerId() == driverKey.getDriverContainerId()
            && relationship.getVolumeId() == driverKey.getVolumeId()
            && relationship.getSnapshotId() == driverKey.getSnapshotId() && relationship
            .getDriverType()
            .equals(driverKey.getDriverType().name())) {
          existInRelationshipStore = true;
          relationship2Apply = relationship;
        }
      }
      if (!existInRelationshipStore) {
        logger.debug("The rule {} is not being applied to driver {} before", iscsiAccessRule,
            driverKey);
        relationship2Apply = new Iscsi2AccessRuleRelationship();
        relationship2Apply.setRelationshipId(RequestIdBuilder.get());
        relationship2Apply.setRuleId(toApplyRuleId);
        relationship2Apply.setDriverContainerId(driverKey.getDriverContainerId());
        relationship2Apply.setDriverType(driverKey.getDriverType().name());
        relationship2Apply.setSnapshotId(driverKey.getSnapshotId());
        relationship2Apply.setVolumeId(driverKey.getVolumeId());
        relationship2Apply.setStatus(AccessRuleStatusBindingVolume.FREE);
        relationshipList.add(relationship2Apply);
      }
      /*
       * Turn all relationship to a proper status base on state machine below.
       * A state machine to transfer status of relationship to proper status.
       */
      switch (relationship2Apply.getStatus()) {
        case FREE:
          if (request.isCommit()) {
            relationship2Apply.setStatus(AccessRuleStatusBindingVolume.APPLIED);
          } else {
            relationship2Apply.setStatus(AccessRuleStatusBindingVolume.APPLING);
          }
          // status of relationship has changed, save it to db
          logger.debug("FREE, save {}", relationship2Apply.getStatus());
          iscsiRuleRelationshipStore.save(relationship2Apply.toIscsiRuleRelationshipInformation());
          break;
        case APPLING:
          if (request.isCommit()) {
            relationship2Apply.setStatus(AccessRuleStatusBindingVolume.APPLIED);
            logger.debug("APPLYING , save {}", relationship2Apply.getStatus());
            iscsiRuleRelationshipStore
                .save(relationship2Apply.toIscsiRuleRelationshipInformation());
          }
          break;
        case APPLIED:
          logger.debug("APPLIED, DO NOTHING");
          // do nothing
          break;
        case CANCELING:
          logger.debug("CANCELING, ERROR");
          IscsiAccessRuleThrift iscsiAccessRuleToRemote = RequestResponseHelper
              .buildIscsiAccessRuleThriftFrom(iscsiAccessRule);
          iscsiAccessRuleToRemote
              .setStatus(AccessRuleStatusThrift.valueOf(relationship2Apply.getStatus().name()));
          response.addToAirDriverKeyList(driverKey);
          break;
        default:
          logger.warn("Unknown status {}", relationship2Apply.getStatus());
          break;
      }
    }

    if (response.getAirDriverKeyList() != null && !response.getAirDriverKeyList().isEmpty()) {
      logger.warn(
          "Unable to apply access rules to specified iscsi in request, due to exist other "
              + "operation on the rules {}",
          response.getAirDriverKeyList());
      return response;
    }

    logger.warn("applyIscsiAccessRuleOnIscsis response: {}", response);
    return response;
  }

  /**
   * An implementation of interface which to cancel access rule from specified iscsi in request.
   */
  @Override
  public CancelIscsiAccessRuleAllAppliedResponse cancelIscsiAccessRuleAllApplied(
      CancelIscsiAccessRuleAllAppliedRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      AccessRuleNotAppliedThrift,
      IscsiAccessRuleUnderOperationThrift, IscsiAccessRuleNotFoundThrift, TException {
    //check the instance status
    checkInstanceStatus("cancelIscsiAccessRuleAllApplied");
    logger.warn("cancelIscsiAccessRuleAllApplied request: {}", request);

    CancelIscsiAccessRuleAllAppliedResponse response = new CancelIscsiAccessRuleAllAppliedResponse(
        request.getRequestId());

    if (request.getDriverKeys() == null || request.getDriverKeysSize() == 0) {
      logger.error("given drive is null");
      return response;
    }

    long toCancelRuleId = request.getRuleId();
    IscsiAccessRuleInformation iscsiAccessRuleInformation = iscsiAccessRuleStore
        .get(toCancelRuleId);
    if (iscsiAccessRuleInformation == null) {
      logger.error("The iscsi access rule not found error");
      throw new IscsiAccessRuleNotFoundThrift();
    }
    IscsiAccessRule iscsiAccessRule = new IscsiAccessRule(iscsiAccessRuleInformation);
    // unable cancel deleting access rule from volume
    if (iscsiAccessRule.getStatus() == AccessRuleStatus.DELETING) {
      logger.error("The iscsi access rule {} is deleting now, unable to cancel", iscsiAccessRule);
      throw new IscsiAccessRuleUnderOperationThrift();
    }
    // turn relationship info got from db to relationship structure
    List<IscsiRuleRelationshipInformation> relationshipInfoList = iscsiRuleRelationshipStore
        .getByRuleId(toCancelRuleId);

    for (DriverKeyThrift driverKeyValue : request.getDriverKeys()) {
      DriverKey driverKey = RequestResponseHelper.buildDriverKeyFrom(driverKeyValue);
      Iscsi2AccessRuleRelationship relationship = null;
      if (relationshipInfoList != null && relationshipInfoList.size() > 0) {
        for (IscsiRuleRelationshipInformation relationshipInfo : relationshipInfoList) {
          if (relationshipInfo.getDriverContainerId() == driverKey.getDriverContainerId()
              && relationshipInfo.getVolumeId() == driverKey.getVolumeId()
              && relationshipInfo.getSnapshotId() == driverKey.getSnapshotId() && relationshipInfo
              .getDriverType().equalsIgnoreCase(driverKey.getDriverType().name())) {
            relationship = new Iscsi2AccessRuleRelationship(relationshipInfo);
            break;
          }
        }
      }
      if (relationship == null) {
        logger.error("access rule not applied");
        throw new AccessRuleNotAppliedThrift();
      }

      // turn status of relationship to proper status after action "cancel"
      switch (relationship.getStatus()) {
        case FREE:
          // do nothingin
          break;
        case APPLING:
          IscsiAccessRuleThrift iscsiAccessRuleToRemote = RequestResponseHelper
              .buildIscsiAccessRuleThriftFrom(iscsiAccessRule);
          iscsiAccessRuleToRemote
              .setStatus(AccessRuleStatusThrift.valueOf(relationship.getStatus().name()));
          response.addToAirDriverKeyList(driverKeyValue);
          break;
        case APPLIED:
          if (request.isCommit()) {
            iscsiRuleRelationshipStore.deleteByRuleIdandDriverKey(driverKey, toCancelRuleId);
          } else {
            relationship.setStatus(AccessRuleStatusBindingVolume.CANCELING);
            iscsiRuleRelationshipStore.save(relationship.toIscsiRuleRelationshipInformation());
          }
          break;
        case CANCELING:
          if (request.isCommit()) {
            iscsiRuleRelationshipStore.deleteByRuleIdandDriverKey(driverKey, toCancelRuleId);
          }
          break;
        default:
          logger.warn("unknown status {} of relationship", relationship.getStatus());
          break;
      }
    }

    logger.warn("cancelIscsiAccessRuleAllApplied response: {}", response);
    return response;
  }

  /**
   * when umount driver should delete the relationship with iscsiAccessRules.
   */
  @Override
  public CancelDriversRulesResponse cancelDriversRules(CancelDriversRulesRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {
    //check the instance status
    checkInstanceStatus("cancelDriversRules");
    logger.warn("CancelDriversRules request: {}", request);

    CancelDriversRulesResponse response = new CancelDriversRulesResponse(request.getRequestId());

    if (request.getDriverKey() == null) {
      return response;
    }

    try {
      //TODO should not use thrift internal
      iscsiRuleRelationshipStore
          .deleteByDriverKey(RequestResponseHelper.buildDriverKeyFrom(request.getDriverKey()));
    } catch (Exception e) {
      logger.error("iscsiRuleRelationship deleteByDriverKey: {} failed", request);
      throw e;
    }

    logger.debug("CancelDriversRules response: {}", response);
    return response;
  }

  /**
   * An implementation of interface.
   * {@link InformationCenter.Iface#listVolumeAccessRules(ListVolumeAccessRulesRequest)}
   * which list all created access rules.
   */
  @Override
  public ListIscsiAccessRulesResponse listIscsiAccessRules(ListIscsiAccessRulesRequest request)
      throws ServiceIsNotAvailableThrift, ServiceHavingBeenShutdownThrift, TException {
    //check the instance status
    checkInstanceStatus("listIscsiAccessRules");
    logger.warn("listIscsiAccessRules request: {}", request);

    ListIscsiAccessRulesResponse listIscsiAccessRulesResponse = new ListIscsiAccessRulesResponse();
    listIscsiAccessRulesResponse.setRequestId(request.getRequestId());

    List<IscsiAccessRuleInformation> iscsiAccessRuleInformations = iscsiAccessRuleStore.list();
    if (iscsiAccessRuleInformations != null && iscsiAccessRuleInformations.size() > 0) {
      for (IscsiAccessRuleInformation iscsiAccessRuleInformation : iscsiAccessRuleInformations) {
        listIscsiAccessRulesResponse.addToAccessRules(RequestResponseHelper
            .buildIscsiAccessRuleThriftFrom(new IscsiAccessRule(iscsiAccessRuleInformation)));
      }
    }
    logger.warn("listVolumeAccessRules response: {}", listIscsiAccessRulesResponse);
    return listIscsiAccessRulesResponse;
  }

  @Override
  public ListIscsiAccessRulesByDriverKeysResponse listIscsiAccessRulesByDriverKeys(
      ListIscsiAccessRulesByDriverKeysRequest request)
      throws ServiceIsNotAvailableThrift, TException {
    //check the instance status
    checkInstanceStatus("listIscsiAccessRulesByDriverKeys");
    logger.warn("ListIscsiAccessRulesByDriverKeysRequest request: {}", request);

    ListIscsiAccessRulesByDriverKeysResponse listIscsiAccessRulesByDriverKeysResponse =
        new ListIscsiAccessRulesByDriverKeysResponse();
    listIscsiAccessRulesByDriverKeysResponse.setRequestId(request.getRequestId());
    Set<DriverKeyThrift> driverKeys = request.getDriverKeys();

    List<IscsiRuleRelationshipInformation> iscsiRuleRelationshipInformationList =
        iscsiRuleRelationshipStore
            .list();
    List<IscsiAccessRuleInformation> iscsiAccessRuleInformations = iscsiAccessRuleStore.list();

    for (DriverKeyThrift driverKey : driverKeys) {
      List<IscsiAccessRuleThrift> iscsiAccessRuleThriftList = new ArrayList<>();
      for (IscsiRuleRelationshipInformation iscsiRule : iscsiRuleRelationshipInformationList) {
        if (driverKey.getDriverContainerId() == iscsiRule.getDriverContainerId()
            && driverKey.getVolumeId() == iscsiRule.getVolumeId()
            && driverKey.getSnapshotId() == iscsiRule
            .getSnapshotId() && driverKey.getDriverType().name()
            .equals(iscsiRule.getDriverType())) {
          Long ruleId = iscsiRule.getRuleId();
          for (IscsiAccessRuleInformation rule : iscsiAccessRuleInformations) {
            if (ruleId == rule.getRuleId()) {
              iscsiAccessRuleThriftList.add(RequestResponseHelper
                  .buildIscsiAccessRuleThriftFrom(new IscsiAccessRule(rule)));
            }
          }
        }
      }
      listIscsiAccessRulesByDriverKeysResponse
          .putToAccessRulesTable(driverKey, iscsiAccessRuleThriftList);
    }

    logger.warn("ListIscsiAccessRulesByDriverKeysResponse response: {}",
        listIscsiAccessRulesByDriverKeysResponse);
    return listIscsiAccessRulesByDriverKeysResponse;
  }

  /* when move volume online, first create access rule for new volume ***/
  @Override
  public CreateAccessRuleOnNewVolumeResponse createAccessRuleOnNewVolume(
      CreateAccessRuleOnNewVolumeRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      InvalidInputExceptionThrift,
      VolumeNotFoundExceptionThrift, VolumeNameExistedExceptionThrift, TException {
    if (shutDownFlag) {
      logger.warn(
          "Cannot deal with any request due to service driver container is being shutdown ...");
      throw new ServiceHavingBeenShutdownThrift();
    }

    logger.warn("UpdateAccessRuleWhenMoveVolume {}", request);
    CreateAccessRuleOnNewVolumeResponse response = new CreateAccessRuleOnNewVolumeResponse();
    response.setRequestId(request.getRequestId());
    long oldVolId = request.getDriver().getVolumeId();
    long newVolId = request.getNewVolumeId();
    // iscsi access rule
    List<IscsiRuleRelationshipInformation> relationshipInfoList = iscsiRuleRelationshipStore
        .getByDriverKey(RequestResponseHelper.buildDriverKeyFrom(request.getDriver()));
    if (relationshipInfoList == null || relationshipInfoList.size() == 0) {
      logger.debug("The driver {} hasn't be applied any access rules", request.getDriver());
    }
    for (IscsiRuleRelationshipInformation relationshipInfo : relationshipInfoList) {
      relationshipInfo.setRelationshipId(RequestIdBuilder.get());
      relationshipInfo.setVolumeId(newVolId);
      iscsiRuleRelationshipStore.save(relationshipInfo);
    }
    // volume access rule
    List<VolumeRuleRelationshipInformation> volRelationshipInfoList = volumeRuleRelationshipStore
        .getByVolumeId(oldVolId);
    if (relationshipInfoList == null || relationshipInfoList.size() == 0) {
      logger.debug("The volume {} hasn't be applied any access rules");
    }
    for (VolumeRuleRelationshipInformation relationshipInfo : volRelationshipInfoList) {
      relationshipInfo.setRelationshipId(RequestIdBuilder.get());
      relationshipInfo.setVolumeId(newVolId);
      volumeRuleRelationshipStore.save(relationshipInfo);
    }
    return response;
  }

  /**
   * when move volume online delete access rule for old volume when delete old volume.
   ***/
  @Override
  public DeleteAccessRuleOnOldVolumeResponse deleteAccessRuleOnOldVolume(
      DeleteAccessRuleOnOldVolumeRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      InvalidInputExceptionThrift,
      VolumeNotFoundExceptionThrift, VolumeNameExistedExceptionThrift, TException {
    if (shutDownFlag) {
      logger.warn(
          "Cannot deal with any request due to service driver container is being shutdown ...");
      throw new ServiceHavingBeenShutdownThrift();
    }
    logger.warn("UpdateAccessRuleWhenMoveVolume {}", request);

    DeleteAccessRuleOnOldVolumeResponse response = new DeleteAccessRuleOnOldVolumeResponse();
    response.setRequestId(request.getRequestId());
    DriverKey driverKey = RequestResponseHelper.buildDriverKeyFrom(request.getDriver());
    long oldVolId = request.getOldVolumeId();
    try {
      // iscsi access rule
      iscsiRuleRelationshipStore
          .deleteByDriverKey(RequestResponseHelper.buildDriverKeyFrom(request.getDriver()));
    } catch (Exception e) {
      logger.error("deleteAccessRuleOnOldVolume deleteByDriverKey: {} failed", request);
      throw e;
    }
    try {
      // volume access rule
      volumeRuleRelationshipStore.deleteByVolumeId(oldVolId);
    } catch (Exception e) {
      logger.error("deleteAccessRuleOnOldVolume deleteByVolumeId: {} failed", request);
      throw e;
    }
    return response;
  }

  @Override
  public GetAppliedIscsisResponse getAppliedIscsis(GetAppliedIscsisRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {
    //check the instance status
    checkInstanceStatus("getAppliedIscsis");
    logger.warn("getAppliedIscsis request: {}", request);

    GetAppliedIscsisResponse response = new GetAppliedIscsisResponse();
    response.setRequestId(request.getRequestId());
    response.setDriverList(new ArrayList<>());

    List<IscsiRuleRelationshipInformation> relationshipInfoList = iscsiRuleRelationshipStore
        .getByRuleId(request.getRuleId());
    if (relationshipInfoList == null || relationshipInfoList.isEmpty()) {
      return response;
    }

    DriverMetadata driverMetadata = null;
    for (IscsiRuleRelationshipInformation relationshipInfo : relationshipInfoList) {
      DriverKeyThrift driverKey = new DriverKeyThrift(relationshipInfo.getDriverContainerId(),
          relationshipInfo.getVolumeId(), relationshipInfo.getSnapshotId(),
          DriverTypeThrift.valueOf(relationshipInfo.getDriverType()));
      try {
        driverMetadata = driverStore
            .get(relationshipInfo.getDriverContainerId(), relationshipInfo.getVolumeId(),
                DriverType.findByName(relationshipInfo.getDriverType()),
                relationshipInfo.getSnapshotId());
      } catch (Exception e) {
        logger.error("Caught an exception", e);
      }

      if (driverMetadata == null) {
        logger.warn("volume is not exist,delete it from rule relation ship table,{}",
            relationshipInfo.getVolumeId());
        //                iscsiRuleRelationshipStore.deleteByDriverKey(driverKey);
        continue;
      }
      response.addToDriverList(RequestResponseHelper.buildThriftDriverMetadataFrom(driverMetadata));
      //response.addToDriverKeyList(new DriverKeyThrift(driverKey));
    }

    logger.warn("getAppliedIscsis response: {}", response);
    return response;
  }

  /******** Iscsis info end .   *************/

  @Override
  public SetIscsiChapControlResponseThrift setIscsiChapControl(
      SetIscsiChapControlRequestThrift request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      NetworkErrorExceptionThrift,
      TException {

    //check the instance status
    checkInstanceStatus("setIscsiChapControl");
    logger.warn("setIscsiChapControl request: {}", request);

    SetIscsiChapControlResponseThrift response = new SetIscsiChapControlResponseThrift();
    response.setRequestId(request.getRequestId());

    // drivercontainer process
    Instance driverContainer = instanceStore.get(new InstanceId(request.getDriverContainerId()));
    EndPoint endPoint = driverContainer.getEndPoint();
    try {
      DriverContainerServiceBlockingClientWrapper clientWrapper = driverContainerClientFactory
          .build(endPoint);
      response = clientWrapper.getClient().setIscsiChapControl(request);
    } catch (GenericThriftClientFactoryException e) {
      logger.error("catch an exception", e);
      throw new NetworkErrorExceptionThrift().setDetail(e.getMessage());
    } catch (ServiceHavingBeenShutdownThrift e) {
      logger.error("caught an exception", e);
      throw e;
    } catch (TException e) {
      logger.error("Caught an exception", e);
      throw filterTexception(e);
    } catch (Exception e) {
      logger.error("Caught an exception", e);
      throw new TException(e);
    }

    DriverMetadata driverMetadata = driverStore
        .get(request.getDriverContainerId(), request.getVolumeId(),
            DriverType.valueOf(request.getDriverType().name()), request.getSnapshotId());

    if (driverMetadata != null) {
      driverMetadata.setChapControl(request.getChapControl());
    } else {
      logger.error("setIscsiChapControl can not find any driver by request:{}", request);
      throw new DriverNotFoundExceptionThrift();
    }

    driverStore.save(driverMetadata);

    logger.warn("setIscsiChapControl response: {} driverMetadata {}", response, driverMetadata);
    return response;
  }

  /**
   * The method use to get all applied successfully accuessRules for lio or iet by driverKey.
   */
  @Override
  public ListIscsiAppliedAccessRulesResponseThrift listIscsiAppliedAccessRules(
      ListIscsiAppliedAccessRulesRequestThrift request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      DriverContainerIsIncExceptionThrift,
      NetworkErrorExceptionThrift, TException {

    //check the instance status
    checkInstanceStatus("listIscsiAppliedAccessRules");
    logger.warn("listIscsiAppliedAccessRules request: {}", request);

    ListIscsiAppliedAccessRulesResponseThrift response =
        new ListIscsiAppliedAccessRulesResponseThrift();
    response.setRequestId(request.getRequestId());

    Set<Instance> driverContainerInstances = instanceStore
        .getAll(PyService.DRIVERCONTAINER.getServiceName());
    Instance chooseDriverContainer = null;
    for (Instance instance : driverContainerInstances) {
      if (instance.getId().getId() == request.getDriverKey().getDriverContainerId()) {
        if (instance.getStatus() == InstanceStatus.HEALTHY) {
          chooseDriverContainer = instance;
        } else {
          throw new DriverContainerIsIncExceptionThrift();
        }
      }
    }
    if (chooseDriverContainer == null) {
      return response;
    }

    try {
      DriverContainerServiceBlockingClientWrapper dcClientWrapper = driverContainerClientFactory
          .build(chooseDriverContainer.getEndPoint(), 50000);
      response = dcClientWrapper.getClient().listIscsiAppliedAccessRules(request);
    } catch (GenericThriftClientFactoryException e) {
      logger.warn("Catch an exception:", e);
      throw new NetworkErrorExceptionThrift().setDetail(e.getMessage());
    } catch (TException e) {
      logger.warn("Catch an exception :", e);
      throw filterTexception(e);
    } catch (Exception e) {
      logger.warn("Catch an exception :", e);
      throw new TException(e);
    }

    logger.warn("listIscsiAppliedAccessRules response: {}", response);
    return response;
  }

  /******** Domain info begin.    *************/
  @Override
  public CreateDomainResponse createDomain(CreateDomainRequest request)
      throws ServiceHavingBeenShutdownThrift, InvalidInputExceptionThrift,
      ServiceIsNotAvailableThrift,
      DomainExistedExceptionThrift, DomainNameExistedExceptionThrift,
      DatanodeNotFreeToUseExceptionThrift,
      DatanodeNotFoundExceptionThrift, DatanodeIsUsingExceptionThrift,
      PermissionNotGrantExceptionThrift,
      AccountNotFoundExceptionThrift, TException {
    //check the instance status
    checkInstanceStatus("createDomain");
    logger.warn("createDomain request: {}", request);
    securityManager.hasPermission(request.getAccountId(), "createDomain");

    CreateDomainResponse createDomainResponse = new CreateDomainResponse();
    String createdDomainName;
    try {

      Domain domain = RequestResponseHelper.buildDomainFrom(request.getDomain());
      if (domain == null) {
        logger.warn("Invalid request, should check it");
        throw new InvalidInputExceptionThrift();
      }
      // check domain name
      if (domain.getDomainName() == null || domain.getDomainName().length() > 100 || domain
          .getDomainName().trim().isEmpty()) {
        logger.warn("Invalid domain name:{}", domain.getDomainName());
        throw new InvalidInputExceptionThrift();
      }
      // check storage pool ids, when create one domain, it can not carry with storage pool for now
      if (!domain.getStoragePools().isEmpty()) {
        logger.error("Invalid storage pool info, can not be here, request:{}", request);
        throw new InvalidInputExceptionThrift();
      }
      // should check duplicate domainName and duplicate domainId
      List<Domain> allDomains;
      try {
        allDomains = domainStore.listAllDomains();
      } catch (SQLException | IOException e) {
        logger.error("caught an exception", e);
        throw new TException();
      }
      for (Domain domainFromMem : allDomains) {
        if (domainFromMem.getDomainName().contentEquals(domain.getDomainName())) {
          logger.warn("domain name: {} has already exists", domain.getDomainName());
          throw new DomainNameExistedExceptionThrift();
        } else if (domainFromMem.getDomainId().equals(domain.getDomainId())) {
          logger.warn("domain Id: {} has already exists", domain.getDomainId());
          throw new DomainExistedExceptionThrift();
        }
      }
      List<Long> addedDatanode = new ArrayList<>();
      if (request.getDomain().isSetDatanodes()) {
       
        for (Long datanodeId : domain.getDataNodes()) {
          InstanceMetadata datanode = storageStore.get(datanodeId);
          if (datanode == null) {
            logger.error("can not find datanode by id:{}", datanodeId);
            throw new DatanodeNotFoundExceptionThrift();
          }
          if (!datanode.isFree()) {
            logger.error("datanode:{} not free now", datanode);
            throw new DatanodeNotFreeToUseExceptionThrift();
          }
          addedDatanode.add(datanodeId);
         
          datanode.setDomainId(domain.getDomainId());
          storageStore.save(datanode);
        }
      }
      domain.setLastUpdateTime(System.currentTimeMillis());
      domainStore.saveDomain(domain);
      createDomainResponse.setAddedDatanodes(addedDatanode);
      createDomainResponse.setDomainThrift(RequestResponseHelper.buildDomainThriftFrom(domain));

      long createdDomainId = request.getDomain().getDomainId();
      createdDomainName = request.getDomain().getDomainName();
      PyResource domainResource = new PyResource(createdDomainId, createdDomainName,
          PyResource.ResourceType.Domain.name());
      securityManager.saveResource(domainResource);
      securityManager.addResource(request.getAccountId(), domainResource);

      for (Long datanodeId : createDomainResponse.getAddedDatanodes()) {
        EndPoint endPoint = null;
        SetDatanodeDomainRequestThrift setDomainRequest = null;
        try {
         
          int tryGetDatanodeClientTime = 3;
          Instance datanode = null;
          while (tryGetDatanodeClientTime > 0) {
            tryGetDatanodeClientTime--;
            datanode = instanceStore.get(new InstanceId(datanodeId));
            if (datanode != null) {
              break;
            } else {
              // try to sleep 100ms
              try {
                Thread.sleep(100);
              } catch (InterruptedException e) {
                logger.warn("caught an exception", e);
              }
            }
          }
          if (datanode == null) {
            logger.warn("can not get datanode:{} instance for create domain", datanodeId);
            continue;
          }
          endPoint = datanode.getEndPoint();
          logger.warn("generateSyncClient endPoint: {}", endPoint);
          final DataNodeService.Iface dataNodeClient = dataNodeClientFactory
              .generateSyncClient(endPoint, timeout);
          setDomainRequest = new SetDatanodeDomainRequestThrift();
          setDomainRequest.setRequestId(RequestIdBuilder.get());
          setDomainRequest.setDomainId(createdDomainId);
          logger.warn("gonna to send request:{} to datanode:[endpoint:{}]<==>[instanceId:{}]",
              request,
              endPoint, datanodeId);
          dataNodeClient.setDatanodeDomain(setDomainRequest);
        } catch (ServiceHavingBeenShutdownThrift | GenericThriftClientFactoryException
            | InvalidInputExceptionThrift e) {
          logger.error("request:{} to datanode:{} failed ", setDomainRequest, endPoint, e);
          continue;
        }
      }
    } catch (TException e) {
      logger.error("Caught an exception", e);
      throw filterTexception(e);
    } catch (Exception e) {
      logger.error("caught an exception", e);
      throw new TException(e);
    }

    buildEndOperationWrtCreateDomainAndSaveToDb(request.getAccountId(), createdDomainName);
    logger.warn("createDomain response: {}", createDomainResponse);
    return createDomainResponse;
  }

  @Override
  public UpdateDomainResponse updateDomain(UpdateDomainRequest request)
      throws ServiceHavingBeenShutdownThrift, InvalidInputExceptionThrift,
      ServiceIsNotAvailableThrift,
      DatanodeNotFreeToUseExceptionThrift, DatanodeNotFoundExceptionThrift,
      DomainNotExistedExceptionThrift,
      DomainIsDeletingExceptionThrift, InstanceIsSubHealthExceptionThrift, TException {
    //check the instance status
    checkInstanceStatus("updateDomain");
    logger.warn("updateDomain request: {}", request);
    securityManager.hasPermission(request.getAccountId(), "updateDomain");
    securityManager.hasRightToAccess(request.getAccountId(), request.getDomain().getDomainId());

    UpdateDomainResponse updateDomainResponse = new UpdateDomainResponse();
    updateDomainResponse.setRequestId(request.getRequestId());

    Domain domainToBeUpdate = RequestResponseHelper.buildDomainFrom(request.getDomain());
    if (domainToBeUpdate == null) {
      logger.warn("Invalid request, should check it");
      throw new InvalidInputExceptionThrift();
    }
    Domain domainExist = null;
    try {
      domainExist = domainStore.getDomain(domainToBeUpdate.getDomainId());
    } catch (Exception e) {
      logger.warn("can not get domain", e);
    }
    if (domainExist == null) {
      logger.error("original domain not exist, domain Id:{}", domainToBeUpdate.getDomainId());
      throw new DomainNotExistedExceptionThrift();
    }

    if (domainExist.getStatus() == Status.Deleting) {
      logger.error("domain:{} is deleting", domainExist);
      throw new DomainIsDeletingExceptionThrift();
    }

    String newDomainName = domainToBeUpdate.getDomainName();
    if (!domainExist.getDomainName().equals(newDomainName)) {
      // if has new domain name is changed, should check name is unique
      List<Domain> domains;
      try {
        domains = domainStore.listAllDomains();
      } catch (Exception e) {
        logger.warn("can not list domain", e);
        throw new InvalidInputExceptionThrift();
      }
      for (Domain domain : domains) {
        if (!domain.getDomainId().equals(domainToBeUpdate.getDomainId()) && domain.getDomainName()
            .equals(newDomainName)) {
          logger.error("update domain name is exist, new domain name:{}, exist domain:{}",
              newDomainName,
              domain);
          throw new InvalidInputExceptionThrift();
        }
      }

      // since console do not transfer storagepool id, we fix this.
      updateStoragePoolWithDomainName(domainToBeUpdate);
    }
    List<Long> addedDatanode = new ArrayList<>();
    long logicalSpace = 0;
    long freeSpace = 0;

   
    InstanceIsSubHealthExceptionThrift instanceIsSubHealthExceptionThrift =
        new InstanceIsSubHealthExceptionThrift();
    for (Long datanodeId : domainToBeUpdate.getDataNodes()) {
      if (!domainExist.getDataNodes().contains(datanodeId)) {
        InstanceMetadata datanode = storageStore.get(datanodeId);
        if (datanode == null) {
          logger.error("can not find datanode by id:{}", datanodeId);
          throw new DatanodeNotFoundExceptionThrift();
        }
        if (!datanode.isFree()) {
          logger.error("datanode:{} not free now", datanode);
          throw new DatanodeNotFreeToUseExceptionThrift();
        }

        Instance datanodeInstance = instanceStore.get(new InstanceId(datanodeId));
        //nstance sdInstance = instanceStore.getByHostNameAndServiceName(
        //datanodeInstance.getEndPointByServiceName(PortType.CONTROL).getHostName(),
        //PyService.SYSTEMDAEMON.getServiceName());
        Instance sdInstance = null;
        if (sdInstance != null && sdInstance.isNetSubHealth()) {
          instanceIsSubHealthExceptionThrift.addToInstanceMetadata(
              RequestResponseHelper.buildInstanceMetadataThriftFrom(datanodeInstance));
          continue;
        }

        addedDatanode.add(datanodeId);
        logicalSpace += datanode.getLogicalCapacity();
        freeSpace += datanode.getFreeSpace();
       
        datanode.setDomainId(domainToBeUpdate.getDomainId());
        storageStore.save(datanode);
        domainToBeUpdate.setLastUpdateTime(System.currentTimeMillis());
      }
    }

    if (instanceIsSubHealthExceptionThrift.getInstanceMetadata() != null) {
      for (InstanceMetadataThrift subHealThDatanode : instanceIsSubHealthExceptionThrift
          .getInstanceMetadata()) {
        domainToBeUpdate.deleteDatanode(subHealThDatanode.getInstanceId());
      }
    }

    domainToBeUpdate.setLogicalSpace(domainExist.getLogicalSpace() + logicalSpace);
    domainToBeUpdate.setFreeSpace(domainExist.getFreeSpace() + freeSpace);
    for (Long datanodeId : domainExist.getDataNodes()) {
      domainToBeUpdate.addDatanode(datanodeId);
    }
    for (Long storagePoolId : domainExist.getStoragePools()) {
      domainToBeUpdate.addStoragePool(storagePoolId);
    }
    domainStore.saveDomain(domainToBeUpdate);
    updateDomainResponse.setAddedDatanodes(addedDatanode);

    if (instanceIsSubHealthExceptionThrift.getInstanceMetadata() != null) {
      logger.error("some instance  {}net is subHealth",
          instanceIsSubHealthExceptionThrift.getInstanceMetadata());
      throw instanceIsSubHealthExceptionThrift;
    }

    try {
      securityManager.updateResourceName(request.getDomain().getDomainId(),
          request.getDomain().getDomainName());
    } catch (Exception e) {
      logger.error("caught an exception", e);
      throw new TException(e);
    }

    for (Long datanodeId : updateDomainResponse.getAddedDatanodes()) {
      // notify datanode to free its' all archives
      EndPoint endPoint = null;
      SetDatanodeDomainRequestThrift setDomainRequest = null;
      try {
        endPoint = instanceStore.get(new InstanceId(datanodeId)).getEndPoint();
        logger.warn("generateSyncClient endPoint: {}", endPoint);
        final DataNodeService.Iface dataNodeClient = dataNodeClientFactory
            .generateSyncClient(endPoint, timeout);
        setDomainRequest = new SetDatanodeDomainRequestThrift();
        setDomainRequest.setRequestId(RequestIdBuilder.get());
        setDomainRequest.setDomainId(request.getDomain().getDomainId());
        logger.warn("gonna to send request:{} to datanode:{}", request, endPoint);
        dataNodeClient.setDatanodeDomain(setDomainRequest);
      } catch (ServiceHavingBeenShutdownThrift | GenericThriftClientFactoryException
          | InvalidInputExceptionThrift e) {
        logger.error("request:{} to datanode:{} failed ", setDomainRequest, endPoint, e);
        continue;
      }
    }

    buildEndOperationWrtModifyDomainAndSaveToDb(request.getAccountId(),
        request.getDomain().getDomainName());
    logger.warn("updateDomain response: {}", updateDomainResponse);
    return updateDomainResponse;
  }

  @Override
  public DeleteDomainResponse deleteDomain(DeleteDomainRequest request)
      throws ServiceHavingBeenShutdownThrift, InvalidInputExceptionThrift,
      ServiceIsNotAvailableThrift,
      DomainNotExistedExceptionThrift, StillHaveStoragePoolExceptionThrift,
      DomainIsDeletingExceptionThrift,
      ResourceNotExistsExceptionThrift, PermissionNotGrantExceptionThrift,
      AccountNotFoundExceptionThrift,
      AccessDeniedExceptionThrift, TException {
    //check the instance status
    checkInstanceStatus("deleteDomain");
    logger.warn("deleteDomain request: {}", request);
    securityManager.hasPermission(request.getAccountId(), "deleteDomain");
    securityManager.hasRightToAccess(request.getAccountId(), request.getDomainId());

    DeleteDomainResponse deleteDomainResponse = new DeleteDomainResponse();
    try {

      Domain domain = null;
      try {
        domain = domainStore.getDomain(request.getDomainId());
      } catch (Exception e) {
        logger.warn("can not get domain", e);
      }
      if (domain == null) {
        logger.warn("don't have domain: {} " + request.getDomainId());
        throw new DomainNotExistedExceptionThrift();
      }
      if (domain.getStatus() == Status.Deleting) {
        logger.error("domain:{} is deleting", domain);
        throw new DomainIsDeletingExceptionThrift();
      }
      if (domain.hasStoragePool()) {
        logger.error("still has storage pool:{} in domain:{}", domain.getStoragePools(),
            domain.getDomainId());
        throw new StillHaveStoragePoolExceptionThrift();
      }
      List<Long> removedDatanode = new ArrayList<>();

      // mark all datanodes in domain free
      for (Long datanodeId : domain.getDataNodes()) {
        InstanceMetadata datanode = storageStore.get(datanodeId);
        if (datanode == null) {
          /*
           * if datanode is not available, we can remove this domain any way
           */
          continue;
        }
        removedDatanode.add(datanodeId);
        datanode.setFree();

        storageStore.save(datanode);
      }

      domain.setLastUpdateTime(System.currentTimeMillis());
      domain.setStatus(Status.Deleting);
      domainStore.saveDomain(domain);
      deleteDomainResponse.setRemovedDatanode(removedDatanode);
      deleteDomainResponse.setDomainName(domain.getDomainName());

      for (Long datanodeId : deleteDomainResponse.getRemovedDatanode()) {
       
        EndPoint endPoint = null;
        FreeDatanodeDomainRequestThrift freeDatanodeRequest = null;
        try {
          int tryGetDatanodeClientTime = 3;
          Instance datanode = null;
          while (tryGetDatanodeClientTime > 0) {
            tryGetDatanodeClientTime--;
            datanode = instanceStore.get(new InstanceId(datanodeId));
            if (datanode != null) {
              break;
            } else {
              // try to sleep 100ms
              try {
                Thread.sleep(100);
              } catch (InterruptedException e) {
                logger.warn("caught an exception", e);
              }
            }
          }
          if (datanode == null) {
            logger.warn("can not get datanode:{} instance for create domain", datanodeId);
            continue;
          }
          endPoint = datanode.getEndPoint();
          logger.warn("generateSyncClient endPoint: {}", endPoint);
          final DataNodeService.Iface dataNodeClient = dataNodeClientFactory
              .generateSyncClient(endPoint, timeout);
          freeDatanodeRequest = new FreeDatanodeDomainRequestThrift();
          freeDatanodeRequest.setRequestId(RequestIdBuilder.get());
          logger.warn("gonna to send request:{} to datanode:{}", request, endPoint);
          dataNodeClient.freeDatanodeDomain(freeDatanodeRequest);
        } catch (ServiceHavingBeenShutdownThrift | GenericThriftClientFactoryException
            | InvalidInputExceptionThrift e) {
          logger.error("request:{} to datanode:{} failed ", freeDatanodeRequest, endPoint, e);
          continue;
        }
      }
    } catch (TException e) {
      logger.error("Caught an exception", e);
      throw filterTexception(e);
    } catch (Exception e) {
      logger.error("caught an exception", e);
      throw new TException(e);
    }
    buildOperationAndSaveToDb(request.getAccountId(), request.getDomainId(), OperationType.DELETE,
        TargetType.DOMAIN, deleteDomainResponse.getDomainName(), "", OperationStatus.ACTIVITING, 0L,
        null,
        null);
    logger.warn("deleteDomain response: {}", deleteDomainResponse);
    return deleteDomainResponse;
  }
  /* Domain info end    *************/

  /******** IoLimit info begin .   *************/

  @Override
  public ListDomainResponse listDomains(ListDomainRequest request)
      throws ServiceHavingBeenShutdownThrift, InvalidInputExceptionThrift,
      ServiceIsNotAvailableThrift,
      TException {

    //check the instance status
    checkInstanceStatus("listDomains");
    logger.warn("listDomains request: {}", request);

    //        securityManager.hasPermission(request.getAccountId(), "listDomains");

    Long accountId = request.getAccountId();
    final Set<Long> accessibleResource = securityManager
        .getAccessibleResourcesByType(accountId, PyResource.ResourceType.Domain);
    List<Long> newDomainIds;
    if (request.isSetDomainIds()) {
      newDomainIds = request.getDomainIds().stream().filter(l -> accessibleResource.contains(l))
          .collect(Collectors.toList());
    } else {
      newDomainIds = new ArrayList<>(accessibleResource);
    }

    request.setDomainIds(newDomainIds);

    ListDomainResponse listDomainResponse = new ListDomainResponse();
    listDomainResponse.setRequestId(request.getRequestId());

    try {
      List<Domain> domains = null;
      List<OneDomainDisplayThrift> domainThriftList = new ArrayList<>();

      try {
        domains = domainStore.listDomains(request.getDomainIds());
      } catch (Exception e) {
        logger.error("can not get domains", e);
      }

      if (domains != null && !domains.isEmpty()) {
        for (Domain domain : domains) {
          List<InstanceMetadataThrift> datanodes = new ArrayList<>();
          for (Long datanodeInstanceId : domain.getDataNodes()) {
            InstanceMetadata datanode = storageStore.get(datanodeInstanceId);
            if (datanode != null /*&& datanode.getDatanodeStatus().equals(OK)*/) {
              InstanceMetadataThrift instanceMetadataThrift = RequestResponseHelper
                  .buildInstanceMetadataThriftFrom(datanode);
              // if SD on the same node is network sub healthy, then mark datanode separated status
              EndPoint datanodeEndpoint = EndPoint.fromString(datanode.getEndpoint());
              if (determinDataNodeIsNetworkSubhealthy(datanodeEndpoint.getHostName())) {
                instanceMetadataThrift.setDatanodeStatus(DatanodeStatusThrift.SEPARATED);
              }
              datanodes.add(instanceMetadataThrift);
            } else {
              logger.info("can not find datanode by id:{}", datanodeInstanceId);
            }
          }
          OneDomainDisplayThrift domainDisplay = new OneDomainDisplayThrift();
          domainDisplay.setDomainThrift(RequestResponseHelper.buildDomainThriftFrom(domain));
          domainDisplay.setDatanodes(datanodes);
          domainThriftList.add(domainDisplay);
        }
      }
      listDomainResponse.setDomainDisplays(domainThriftList);

      // this is for delete the resource after domain is deleted and removed from domainStore
      for (Long domainId : newDomainIds) {
        boolean found = false;
        for (OneDomainDisplayThrift domainDisplayThrift : listDomainResponse.getDomainDisplays()) {
          if (domainDisplayThrift.getDomainThrift().getDomainId() == domainId) {
            found = true;
            break;
          }
        }
        if (!found) {
          securityManager.unbindResource(domainId);
          securityManager.removeResource(domainId);
        }
      }
    } catch (TException e) {
      logger.error("Caught an exception", e);
      throw filterTexception(e);
    } catch (Exception e) {
      logger.error("caught an exception", e);
      throw new TException(e);
    }
    logger.warn("listDomains response: {}", listDomainResponse);
    return listDomainResponse;
  }

  @Override
  public RemoveDatanodeFromDomainResponse removeDatanodeFromDomain(
      RemoveDatanodeFromDomainRequest request)
      throws ServiceHavingBeenShutdownThrift, InvalidInputExceptionThrift,
      ServiceIsNotAvailableThrift,
      FailToRemoveDatanodeFromDomainExceptionThrift, DatanodeNotFoundExceptionThrift,
      DomainNotExistedExceptionThrift, DomainIsDeletingExceptionThrift,
      PermissionNotGrantExceptionThrift,
      AccountNotFoundExceptionThrift, AccessDeniedExceptionThrift, TException {
    //check the instance status
    checkInstanceStatus("removeDatanodeFromDomain");
    logger.warn("removeDatanodeFromDomain request: {}", request);
    securityManager.hasPermission(request.getAccountId(), "removeDatanodeFromDomain");
    securityManager.hasRightToAccess(request.getAccountId(), request.getDomainId());

    RemoveDatanodeFromDomainResponse response = new RemoveDatanodeFromDomainResponse();
    try {
      ValidateParam.validateRemoveDatanodeFromDomainRequest(request);
      // check domain id exist
      Domain domain = null;
      try {
        domain = domainStore.getDomain(request.getDomainId());
      } catch (Exception e) {
        logger.warn("can not get domain", e);
      }
      if (domain == null) {
        logger.warn("can't find any domain by id:{}", request.getDomainId());
        throw new DomainNotExistedExceptionThrift();
      }
      if (domain.getStatus() == Status.Deleting) {
        logger.error("domain:{} is deleting", domain);
        throw new DomainIsDeletingExceptionThrift();
      }
      // should remove all archives from storage pools before delete datanode
      Long datanodeId = request.getDatanodeInstanceId();
      InstanceMetadata datanode = storageStore.get(datanodeId);
      if (datanode == null) {
        logger.error("can not find datanode by Id:{}", datanodeId);
        throw new DatanodeNotFoundExceptionThrift();
      }
      if (!domain.getDomainId().equals(datanode.getDomainId())) {
        logger.error("domain:{} not contains datanode:{}", domain.getDomainId(),
            datanode.getInstanceId());
        throw new DatanodeNotFoundExceptionThrift();
      }

      List<StoragePool> storagePoolList;
      try {
        storagePoolList = storagePoolStore.listAllStoragePools();
      } catch (Exception e) {
        logger.error("can not get any storage pools", e);
        throw new TException();
      }
      Validate.notNull(storagePoolList);
      for (RawArchiveMetadata archiveMetadata : datanode.getArchives()) {
        for (StoragePool storagePool : storagePoolList) {
          Long archiveId = archiveMetadata.getArchiveId();
          if (storagePool.getArchivesInDataNode().containsEntry(datanodeId, archiveId)) {
           
           
            archiveMetadata.setFree();
            storagePool.removeArchiveFromDatanode(datanodeId, archiveId);
            storagePool.setLastUpdateTime(System.currentTimeMillis());
            storagePoolStore.saveStoragePool(storagePool);
            break;
          }
        }
      }
     
      datanode.setFree();
      storageStore.save(datanode);
      domain.setLogicalSpace(domain.getLogicalSpace() - datanode.getLogicalCapacity());
      domain.setFreeSpace(domain.getFreeSpace() - datanode.getFreeSpace());

      domain.setLastUpdateTime(System.currentTimeMillis());
      domain.deleteDatanode(datanodeId);
      domainStore.saveDomain(domain);
      response.setRequestId(request.getRequestId());
      response.setDomainName(domain.getDomainName());

      // notify datanode to free its' all archives
      EndPoint endPoint = null;
      FreeDatanodeDomainRequestThrift freeDatanodeRequest = null;
      try {
        endPoint = instanceStore.get(new InstanceId(request.getDatanodeInstanceId())).getEndPoint();
        final DataNodeService.Iface dataNodeClient = dataNodeClientFactory
            .generateSyncClient(endPoint, timeout);
        freeDatanodeRequest = new FreeDatanodeDomainRequestThrift();
        freeDatanodeRequest.setRequestId(RequestIdBuilder.get());
        logger
            .warn("dataNodeClient.freeDatanodeDomain freeDatanodeRequest: {}", freeDatanodeRequest);
        dataNodeClient.freeDatanodeDomain(freeDatanodeRequest);
      } catch (ServiceHavingBeenShutdownThrift | GenericThriftClientFactoryException
          | InvalidInputExceptionThrift e) {
        logger.error("request: {} to datanode: {} failed ", freeDatanodeRequest, endPoint, e);
      }
    } catch (InvalidInputExceptionThrift e) {
      logger.error("Caught an exception", e);
      throw e;
    } catch (TException e) {
      logger.error("Caught an exception", e);
      throw filterTexception(e);
    } catch (Exception e) {
      logger.error("caught an exception", e);
      throw new TException(e);
    }
    buildEndOperationWrtModifyDomainAndSaveToDb(request.getAccountId(), response.getDomainName());
    logger.warn("removeDatanodeFromDomain response: {}", response);
    return response;
  }

  /**
   * An implementation of interface.
   * {@link InformationCenter.Iface#createIoLimitations(CreateIoLimitationsRequest)}
   * which create IoLimitations.
   */
  @Override
  public synchronized CreateIoLimitationsResponse createIoLimitations(
      CreateIoLimitationsRequest request)
      throws ServiceIsNotAvailableThrift, InvalidInputExceptionThrift,
      ServiceHavingBeenShutdownThrift,
      IoLimitationTimeInterLeavingThrift, TException {
    //check the instance status
    checkInstanceStatus("createIoLimitations");
    logger.warn("createIoLimitations request: {}", request);
    securityManager.hasPermission(request.getAccountId(), "createIoLimitations");

    final CreateIoLimitationsResponse response = new CreateIoLimitationsResponse(
        request.getRequestId());
    if (request.getIoLimitation() == null) {
      logger.error("IoLimitation null error");
      throw new InvalidInputExceptionThrift();
    }

    if (request.getIoLimitation().getEntries() == null) {
      logger.error("IoLimitationEntry null error");
      throw new InvalidInputExceptionThrift();
    }

    if (request.getIoLimitation().getLimitType() == LimitTypeThrift.Static
        && request.getIoLimitation().getEntries().size() != 1) {
      logger.error("IoLimitationEntry too many when static error");
      throw new InvalidInputExceptionThrift();
    }

    IoLimitationThrift ioLimitationThrift = request.getIoLimitation();
    IoLimitation toIoLimitation = RequestResponseHelper.buildIoLimitationFrom(ioLimitationThrift);

    //check internal timezone
    boolean conflict = RequestResponseHelper
        .judgeDynamicIoLimitationTimeInterleaving(toIoLimitation);
    if (conflict) {
      logger.error("createIoLimitations timeInterLeaving fail!");
      throw new IoLimitationTimeInterLeavingThrift();
    }

    List<IoLimitation> ioLimitationList = ioLimitationStore.list();
   
    // which rule is already existed in the system
    boolean ioLimitationExisted = false;
    for (IoLimitation ioLimitation : ioLimitationList) {
      if (ioLimitationThrift.getLimitationName().equals(ioLimitation.getName())) {
        logger.error("information:{} is already exists", ioLimitationThrift.getLimitationName());
        throw new IoLimitationsDuplicateThrift();
      }
      if (ioLimitation.getLimitType() == toIoLimitation.getLimitType()
          && ioLimitation.getId() == toIoLimitation
          .getId() && ioLimitation.getName().equals(toIoLimitation.getName()) && listContentEquals(
          ioLimitation.getEntries(), toIoLimitation.getEntries())) {
        ioLimitationExisted = true;
        break;
      } else {
        // do nothing
      }
    }
    if (!ioLimitationExisted) {
      IoLimitation ioLimitation = RequestResponseHelper.buildIoLimitationFrom(ioLimitationThrift);
      ioLimitation.setStatus(IoLimitationStatus.AVAILABLE);
      logger.warn("when createIoLimitations, the value :{}", ioLimitation);
      ioLimitationStore.save(ioLimitation);
    } else {
      // throw an exception out
      logger.error("information center throws an exception IoLimitationsDuplicateThrift");
      throw new IoLimitationsDuplicateThrift();
    }

    buildOperationAndSaveToDb(request.getAccountId(), null, OperationType.CREATE, TargetType.QOS,
        "",
        request.getIoLimitation().getLimitationName(), OperationStatus.SUCCESS, 0L, null, null);
    logger.warn("createIoLimitations response: {}", response);
    return response;
  }

  /**
   * An implementation of interface.
    * {@link InformationCenter.Iface#updateIoLimitations(UpdateIoLimitationRulesRequest)}
   * which create IoLimitations.
   */
  @Override
  public synchronized UpdateIoLimitationsResponse updateIoLimitations(
      UpdateIoLimitationRulesRequest request)
      throws ServiceIsNotAvailableThrift, InvalidInputExceptionThrift, IoLimitationsNotExists,
      IoLimitationTimeInterLeavingThrift, TException {
    //check the instance status
    checkInstanceStatus("updateIoLimitations");
    logger.warn("updateIoLimitations request: {}", request);
    securityManager.hasPermission(request.getAccountId(), "updateIoLimitations");

    UpdateIoLimitationsResponse response = new UpdateIoLimitationsResponse(request.getRequestId());
    if (request.getIoLimitation() == null) {
      logger.debug("No access rule to create in request, nothing to do");
      return response;
    }

    IoLimitationThrift ioLimitationThrift = request.getIoLimitation();
    IoLimitation toIoLimitaion = RequestResponseHelper
        .buildIoLimitationFrom(request.getIoLimitation());

    // old from db
    IoLimitation ioLimitation = ioLimitationStore.get(ioLimitationThrift.getLimitationId());
    if (ioLimitation == null) {
      logger.error("No IoLimitations with id {}", ioLimitationThrift.getLimitationId());
      throw new IoLimitationsNotExists();
    }

    //check internal timezone
    boolean conflict = RequestResponseHelper
        .judgeDynamicIoLimitationTimeInterleaving(toIoLimitaion);
    if (conflict) {
      logger.error("updateIoLimitations timeInterLeaving fail!");
      throw new IoLimitationTimeInterLeavingThrift();
    }

    List<IoLimitation> ioLimitationFromDb = ioLimitationStore.list();
    boolean ioLimitationExisted = false;
    for (IoLimitation limitation : ioLimitationFromDb) {
      if (ioLimitation.getId() != limitation.getId()

          && ioLimitationThrift.getLimitationName().equals(limitation.getName())) {
        logger.error("IoLimitations:{} is already exists", ioLimitationThrift.getLimitationId());
        throw new IoLimitationsDuplicateThrift();
      }
      if (limitation.getLimitType() == toIoLimitaion.getLimitType()
          && limitation.getId() == toIoLimitaion.getId()
          && limitation.getName() == toIoLimitaion.getName() && listContentEquals(
          limitation.getEntries(),
          toIoLimitaion.getEntries())) {
        ioLimitationExisted = true;
        break;
      } else {
        // do nothing
      }
    }
    if (!ioLimitationExisted) {
      logger.warn("when updateIoLimitations, valume {} to {}", ioLimitation, toIoLimitaion);
      toIoLimitaion.setStatus(IoLimitationStatus.AVAILABLE);
      ioLimitationStore.update(toIoLimitaion);
    } else {
      // throw an exception out
      logger.error("information center throws an exception IoLimitationsDuplicateThrift");
      throw new IoLimitationsDuplicateThrift();
    }

    buildOperationAndSaveToDb(request.getAccountId(), null, OperationType.MODIFY, TargetType.QOS,
        request.getIoLimitation().getLimitationName(), "", OperationStatus.SUCCESS, 0L, null, null);
    logger.warn("UpdateIoLimitationsResponse response: {}", response);
    return response;
  }

  @Override
  public GetLimitsResponse getLimits(GetLimitsRequest request)
      throws DriverNotFoundExceptionThrift, ServiceIsNotAvailableThrift, TException {
    checkInstanceStatus("getLimits");
    logger.warn("getLimits request: {}", request);

    GetLimitsResponse response = new GetLimitsResponse();
    return response;
  }

  @Override
  public AddOrModifyIoLimitResponse addOrModifyIoLimit(AddOrModifyIoLimitRequest request)
      throws DriverNotFoundExceptionThrift, InvalidInputExceptionThrift,
      ServiceIsNotAvailableThrift,
      AlreadyExistStaticLimitationExceptionThrift,
      DynamicIoLimitationTimeInterleavingExceptionThrift,
      TException {
    //check the instance status
    checkInstanceStatus("addOrModifyIoLimit");
    logger.warn("addOrModifyIoLimit request: {}", request);

    IoLimitation updateIoLimitation = RequestResponseHelper
        .buildIoLimitationFrom(request.getIoLimitation());
    final AddOrModifyIoLimitResponse response = new AddOrModifyIoLimitResponse();
    if (!updateIoLimitation.validate()) {
      logger.error("invalid IoLimitation value modifying : {}", updateIoLimitation);
      throw new InvalidInputExceptionThrift();
    }

    String volumeName = "";
    String driverType;
    VolumeMetadata volumeMetadata = volumeStore.getVolume(request.getVolumeId());
    if (volumeMetadata != null) {
      volumeName = volumeMetadata.getName();
    }
    DriverMetadata driver = driverStore.get(request.getDriverContainerId(), request.getVolumeId(),
        DriverType.valueOf(request.getDriverType().name()), request.getSnapshotId());
    EndPoint endPoint = null;
    if (driver != null) {
      try {
        driverStore.updateIoLimit(request.getDriverContainerId(), request.getVolumeId(),
            DriverType.valueOf(request.getDriverType().name()), request.getSnapshotId(),
            updateIoLimitation);
      } catch (Exception e) {
        logger.error("caught an exception", e);
        throw e;
      }
      try {
        String hostName = driver.getHostName();
        int port = driver.getCoordinatorPort();
        endPoint = EndPointParser.parseLocalEndPoint(port, hostName);
        final Coordinator.Iface coordinatorClient = coordinatorClientFactory.build(endPoint)
            .getClient();
        AddOrModifyLimitationRequest modifyRequest = new AddOrModifyLimitationRequest();
        modifyRequest.setIoLimitation(request.getIoLimitation());
        modifyRequest.setRequestId(RequestIdBuilder.get());
        modifyRequest.setVolumeId(request.getVolumeId());
        coordinatorClient.addOrModifyLimitation(modifyRequest);
      } catch (Exception e) {
        logger.warn("cannot update io limit:{} to the driver {}", updateIoLimitation, driver);
      }
    } else {
      logger.error("addOrModifyIoLimit can not find any driver by request:{}", request);
      throw new DriverNotFoundExceptionThrift();
    }

    response.setRequestId(request.getRequestId());
    response.setVolumeName(volumeName);
    if (driver.getDriverType().equals(DriverType.NBD)) {
      driverType = "PYD";
    } else {
      driverType = driver.getDriverType().name();
    }
    response.setDriverTypeAndEndPoint(driverType + " " + endPoint.toString());
    logger.warn("addOrModifyIoLimit response: {}", response);
    return response;
  }

  /******** IoLimit info end.    *************/

  @Override
  public DeleteIoLimitResponse deleteIoLimit(DeleteIoLimitRequest request)
      throws InvalidInputExceptionThrift, ServiceIsNotAvailableThrift, TException,
      DriverNotFoundExceptionThrift {
    //check the instance status
    checkInstanceStatus("deleteIoLimit");
    logger.warn("deleteIoLimit request: {}", request);

    String volumeName = "";
    String driverType;
    VolumeMetadata volumeMetadata = volumeStore.getVolume(request.getVolumeId());
    if (volumeMetadata != null) {
      volumeName = volumeMetadata.getName();
    }
    DriverMetadata driver = driverStore.get(request.getDriverContainerId(), request.getVolumeId(),
        DriverType.valueOf(request.getDriverType().name()), request.getSnapshotId());
    EndPoint endPoint = null;
    if (driver != null) {
      try {
        endPoint = EndPointParser
            .parseLocalEndPoint(driver.getCoordinatorPort(), driver.getHostName());
        Coordinator.Iface coordinatorClient = coordinatorClientFactory.build(endPoint).getClient();

        DeleteLimitationRequest deleteRequest = new DeleteLimitationRequest(RequestIdBuilder.get(),
            request.getVolumeId(), request.getLimitId());
        coordinatorClient.deleteLimitation(deleteRequest);
      } catch (Exception e) {
        logger.error("cannot delete io limit for driver {}", driver);
      }
      if (-1 == driverStore.deleteIoLimit(request.getDriverContainerId(), request.getVolumeId(),
          DriverType.valueOf(request.getDriverType().name()), request.getSnapshotId(),
          request.getLimitId())) {
        throw new DriverNotFoundExceptionThrift();
      }
    } else {
      logger.error("deleteIoLimit can not find any driver by request:{}", request);
      throw new DriverNotFoundExceptionThrift();
    }
    if (driver.getDriverType().equals(DriverType.NBD)) {
      driverType = "PYD";
    } else {
      driverType = driver.getDriverType().name();
    }
    DeleteIoLimitResponse response = new DeleteIoLimitResponse(request.getRequestId(), volumeName,
        driverType + " " + endPoint.toString());
    logger.warn("deleteIoLimit response: {}", response);
    return response;
  }

  @Override
  public ChangeLimitTypeResponse changeLimitType(ChangeLimitTypeRequest request)
      throws VolumeNotFoundExceptionThrift, InvalidInputExceptionThrift,
      ServiceIsNotAvailableThrift,
      DriverNotFoundExceptionThrift, TException {
    //check the instance status
    checkInstanceStatus("changeLimitType");
    logger.warn("changeLimitType request: {}", request);

    DriverMetadata driver = driverStore.get(request.getDriverContainerId(), request.getVolumeId(),
        DriverType.valueOf(request.getDriverType().name()), request.getSnapshotId());
    if (driver != null) {
      driverStore.changeLimitType(request.getDriverContainerId(), request.getVolumeId(),
          DriverType.valueOf(request.getDriverType().name()), request.getSnapshotId(),
          request.getLimitId(),
          request.isStaticLimit());
    } else {
      logger.error("changeLimitType can not find any driver by request:{}", request);
      throw new DriverNotFoundExceptionThrift();
    }
    ChangeLimitTypeResponse response = new ChangeLimitTypeResponse(request.getRequestId());
    logger.warn("changeLimitType response: {}", response);
    return response;
  }

  /******** StoragePool begin .   *************/
  @Override
  public CreateStoragePoolResponseThrift createStoragePool(CreateStoragePoolRequestThrift request)
      throws ServiceHavingBeenShutdownThrift, InvalidInputExceptionThrift,
      StoragePoolExistedExceptionThrift,
      StoragePoolNameExistedExceptionThrift, ServiceIsNotAvailableThrift,
      ArchiveNotFreeToUseExceptionThrift,
      ArchiveNotFoundExceptionThrift, DomainNotExistedExceptionThrift,
      ArchiveIsUsingExceptionThrift,
      DomainIsDeletingExceptionThrift, PermissionNotGrantExceptionThrift,
      AccountNotFoundExceptionThrift,
      AccessDeniedExceptionThrift, TException {
    //check the instance status
    checkInstanceStatus("createStoragePool");
    logger.warn("createStoragePool request: {}", request);

    try {
      ValidateParam.validateCreateStoragePoolRequest(request);
    } catch (InvalidInputExceptionThrift e) {
      throw e;
    }

    securityManager.hasPermission(request.getAccountId(), "createStoragePool");
    securityManager
        .hasRightToAccess(request.getAccountId(), request.getStoragePool().getDomainId());

    CreateStoragePoolResponseThrift response = new CreateStoragePoolResponseThrift();
    try {
      StoragePool storagePoolToBeCreate = RequestResponseHelper
          .buildStoragePoolFromThrift(request.getStoragePool());

      // check domain id exist
      Domain domain = null;
      try {
        domain = domainStore.getDomain(storagePoolToBeCreate.getDomainId());
      } catch (Exception e) {
        logger.warn("can not get domain", e);
      }
      if (domain == null) {
        logger.warn("can't find any domain by id:{}", storagePoolToBeCreate.getDomainId());
        throw new DomainNotExistedExceptionThrift();
      }

      if (domain.getStatus() == Status.Deleting) {
        logger.error("domain:{} is deleting", domain);
        throw new DomainIsDeletingExceptionThrift();
      }
      // check pool id not exist
      StoragePool storagePoolExist = null;
      try {
        storagePoolExist = storagePoolStore.getStoragePool(storagePoolToBeCreate.getPoolId());
      } catch (Exception e) {
        logger.warn("can not get storage pool", e);
      }
      if (storagePoolExist != null) {
        throw new StoragePoolExistedExceptionThrift();
      }
      // check name not exist
      List<StoragePool> allStoragePool = null;
      try {
        allStoragePool = storagePoolStore.listAllStoragePools();
      } catch (Exception e) {
        logger.error("can not get storage pools", e);
      }
      if (allStoragePool == null) {
        logger.error("failed to get storage pool");
        throw new ServiceIsNotAvailableThrift();
      }
      if (!allStoragePool.isEmpty()) {
        for (StoragePool storagePool : allStoragePool) {
          if (storagePool.getDomainId().equals(storagePoolToBeCreate.getDomainId()) && storagePool
              .getName()
              .equals(storagePoolToBeCreate.getName())) {
            throw new StoragePoolNameExistedExceptionThrift();
          }
        }
      }

      // can't contain volume ids
      Validate.isTrue(storagePoolToBeCreate.getVolumeIds().isEmpty(),
          "it is impossiable contain volume ids:" + storagePoolToBeCreate.getVolumeIds());

      Map<Long, List<Long>> datanodeMapArchives = new HashMap<>();

      // check if has any archive
      if (!storagePoolToBeCreate.getArchivesInDataNode().isEmpty()) {
        // find all refer datanodes first
        for (Entry<Long, Long> entry : storagePoolToBeCreate.getArchivesInDataNode().entries()) {
          Long datanodeId = entry.getKey();
          Long archiveId = entry.getValue();
          if (!datanodeMapArchives.containsKey(datanodeId)) {
            List<Long> addedArchiveIds = new ArrayList<>();
            datanodeMapArchives.put(datanodeId, addedArchiveIds);
          }

          InstanceMetadata datanode = storageStore.get(datanodeId);
          if (datanode == null) {
            logger.error("can't find datanode by given id:{}, storage:{}", datanodeId,
                storageStore.list());
            throw new ArchiveNotFoundExceptionThrift();
          }
          // check if datanode in this domain
          if (datanode.getDomainId() != null && !domain.getDomainId()
              .equals(datanode.getDomainId())) {
            logger.error(
                "can not add archive:{} to storage pool:{}, because this datanode:{} is not in "
                    + "domain:{}",
                archiveId, storagePoolToBeCreate.getPoolId(), datanodeId, domain.getDomainId());
            throw new ArchiveNotFoundExceptionThrift();
          }

          List<Long> archiveIds = datanodeMapArchives.get(datanodeId);
          RawArchiveMetadata archive = datanode.getArchiveById(archiveId);
          if (archive == null) {
            logger.error("can't find archive:{} at datanode:{}, request:{}", archiveId, datanode,
                request);
            throw new ArchiveNotFoundExceptionThrift();
          } else {
            if (archive.getStoragePoolId() != PoolInfo.AVAILABLE_POOLID) {
              throw new ArchiveNotFreeToUseExceptionThrift();
            } else {
              archiveIds.add(archiveId);
             
              archive.setStoragePoolId(storagePoolToBeCreate.getPoolId());
            }
          }
          storageStore.save(datanode);
        }
      }
      // now we can save to storage pool store
      storagePoolToBeCreate.setLastUpdateTime(System.currentTimeMillis());
      storagePoolToBeCreate.setDomainName(domain.getDomainName());
      storagePoolToBeCreate.setStoragePoolLevel(StoragePoolLevel.HIGH.name());
      storagePoolStore.saveStoragePool(storagePoolToBeCreate);
     
      domain = null;
      try {
        domain = domainStore.getDomain(storagePoolToBeCreate.getDomainId());
      } catch (Exception e) {
        logger.warn("can not get domain", e);
      }
      if (domain == null) {
        logger.error("can not add storage pool:{} to domain", storagePoolToBeCreate.getPoolId());
        throw new DomainNotExistedExceptionThrift();
      }

      domain.addStoragePool(storagePoolToBeCreate.getPoolId());
      domainStore.saveDomain(domain);

      MigrationRule migrationRule = null;
      if (storagePoolToBeCreate.getMigrationRuleId() != StoragePool.DEFAULT_MIGRATION_RULE_ID
          .longValue()) {
        MigrationRuleInformation migrationRuleInformation = migrationRuleStore
            .get(storagePoolToBeCreate.getMigrationRuleId());
        if (migrationRuleInformation != null) {
          migrationRule = migrationRuleInformation.toMigrationRule();
        }
      }

      response.setStoragePoolThrift(
          RequestResponseHelper.buildThriftStoragePoolFrom(storagePoolToBeCreate, migrationRule));
      response.setRequestId(request.getRequestId());
      response.setDatanodeMapAddedArchives(datanodeMapArchives);

      long createdStoragePoolId = request.getStoragePool().getPoolId();
      String createdStoragePoolName = request.getStoragePool().getPoolName();
      PyResource storagePoolResource = new PyResource(createdStoragePoolId, createdStoragePoolName,
          PyResource.ResourceType.StoragePool.name());
      securityManager.saveResource(storagePoolResource);
      securityManager.addResource(request.getAccountId(), storagePoolResource);

      // notify datanode set archives storage pool id
      for (Entry<Long, List<Long>> entry : response.getDatanodeMapAddedArchives().entrySet()) {
        Long datanodeId = entry.getKey();
        List<Long> archiveIdList = entry.getValue();
        EndPoint endPoint = null;
        SetArchiveStoragePoolRequestThrift setStoragePoolRequest = null;
        try {
          setStoragePoolRequest = new SetArchiveStoragePoolRequestThrift();
          setStoragePoolRequest.setRequestId(RequestIdBuilder.get());
          Map<Long, Long> archiveIdMapStoragePoolId = new HashMap<>();
          for (Long archiveId : archiveIdList) {
            archiveIdMapStoragePoolId.put(archiveId, createdStoragePoolId);
          }
          setStoragePoolRequest.setArchiveIdMapStoragePoolId(archiveIdMapStoragePoolId);
          Instance instance = instanceStore.get(new InstanceId(datanodeId));
          if (instance == null) {
            continue;
          }

          endPoint = instance.getEndPoint();
          DataNodeService.Iface dataNodeClient = dataNodeClientFactory
              .generateSyncClient(endPoint, timeout);
          logger.warn("gonna to send request: {} to datanode:[endpoint:{}]<==>[instanceId:{}]",
              setStoragePoolRequest, endPoint, datanodeId);
          logger.warn("dataNodeClient.setArchiveStoragePool setStoragePoolRequest: {}",
              setStoragePoolRequest);
          dataNodeClient.setArchiveStoragePool(setStoragePoolRequest);
        } catch (ServiceHavingBeenShutdownThrift | GenericThriftClientFactoryException
            | InvalidInputExceptionThrift e) {
          logger.error("request: {} to datanode: {} failed ", setStoragePoolRequest, endPoint, e);
          continue;
        }
      }
    } catch (TException e) {
      logger.error("Caught an exception", e);
      throw filterTexception(e);
    } catch (Exception e) {
      logger.error("caught an exception", e);
      throw new TException(e);
    }

    String storagePoolName = request.getStoragePool().getPoolName();
    buildEndOperationWrtCreateStoragePoolAndSaveToDb(request.getAccountId(), storagePoolName);
    logger.warn("createStoragePool response: {}", response);
    return response;
  }

  @Override
  public UpdateStoragePoolResponseThrift updateStoragePool(UpdateStoragePoolRequestThrift request)
      throws ServiceHavingBeenShutdownThrift, InvalidInputExceptionThrift,
      ServiceIsNotAvailableThrift,
      ArchiveNotFreeToUseExceptionThrift, ArchiveNotFoundExceptionThrift,
      StoragePoolNotExistedExceptionThrift,
      DomainNotExistedExceptionThrift, ArchiveIsUsingExceptionThrift,
      StoragePoolIsDeletingExceptionThrift,
      PermissionNotGrantExceptionThrift, AccountNotFoundExceptionThrift,
      AccessDeniedExceptionThrift,
      EndPointNotFoundExceptionThrift, TooManyEndPointFoundExceptionThrift,
      NetworkErrorExceptionThrift,
      TException {

    //check the instance status
    checkInstanceStatus("updateStoragePool");
    logger.warn("updateStoragePool request: {}", request);

    //check input value
    try {
      ValidateParam.validateUpdateStoragePoolRequest(request);
    } catch (InvalidInputExceptionThrift e) {
      throw e;
    }

    long beginTime = System.currentTimeMillis();
    securityManager.hasPermission(request.getAccountId(), "updateStoragePool");
    securityManager
        .hasRightToAccess(request.getAccountId(), request.getStoragePool().getDomainId());
    securityManager.hasRightToAccess(request.getAccountId(), request.getStoragePool().getPoolId());

    UpdateStoragePoolResponseThrift response = new UpdateStoragePoolResponseThrift();
    try {

      StoragePool storagePoolToBeUpdate = RequestResponseHelper
          .buildStoragePoolFromThrift(request.getStoragePool());

      // check domain id exist
      Domain domain = null;
      try {
        domain = domainStore.getDomain(storagePoolToBeUpdate.getDomainId());
      } catch (Exception e) {
        logger.warn("can not get domain", e);
      }
      if (domain == null) {
        logger.warn("can't find any domain by id:{}", storagePoolToBeUpdate.getDomainId());
        throw new DomainNotExistedExceptionThrift();
      }

      // check pool id exist
      StoragePool storagePoolExist = null;
      try {
        storagePoolExist = storagePoolStore.getStoragePool(storagePoolToBeUpdate.getPoolId());
      } catch (Exception e) {
        logger.warn("can not get storage pool", e);
      }
      if (storagePoolExist == null) {
        throw new StoragePoolNotExistedExceptionThrift();
      }

      if (storagePoolExist.getStatus() == Status.Deleting) {
        logger.error("storage pool is deleting, storage pool:{}", storagePoolExist);
        throw new StoragePoolIsDeletingExceptionThrift();
      }

      if (!storagePoolExist.getName().equals(storagePoolToBeUpdate.getName())) {
        // check update name not exist, except original storage pool name
        List<StoragePool> allStoragePool;
        try {
          allStoragePool = storagePoolStore.listAllStoragePools();
        } catch (Exception e) {
          logger.error("can not list storage pools", e);
          throw new ServiceIsNotAvailableThrift();
        }
        Validate.notNull(allStoragePool);
        for (StoragePool storagePool : allStoragePool) {
          if (storagePool.getDomainId().equals(storagePoolToBeUpdate.getDomainId()) && storagePool
              .getName()
              .equals(storagePoolToBeUpdate.getName())) {
            logger.error("update name is exist, exist storage pool:{}, update storage pool:{}",
                storagePool,
                storagePoolToBeUpdate);
            throw new StoragePoolNameExistedExceptionThrift();
          }
        }
      }

      Map<Long, List<Long>> datanodeMapArchives = new HashMap<>();
     
     
      for (Entry<Long, Long> entry : storagePoolToBeUpdate.getArchivesInDataNode().entries()) {
        Long datanodeId = entry.getKey();
        Long archiveId = entry.getValue();
        if (!datanodeMapArchives.containsKey(datanodeId)) {
          List<Long> addedArchiveIds = new ArrayList<>();
          datanodeMapArchives.put(datanodeId, addedArchiveIds);
        }
        if (!storagePoolExist.getArchivesInDataNode().containsEntry(datanodeId, archiveId)) {
          InstanceMetadata datanode = storageStore.get(datanodeId);
          if (datanode == null) {
            logger.error("can't find datanode by given id:{}, storage:{}", datanodeId,
                storageStore.list());
            throw new ArchiveNotFoundExceptionThrift();
          }

          // check if datanode in this domain
          if (!domain.getDomainId().equals(datanode.getDomainId())) {
            logger.error(
                "can not add archive:{} to storage pool:{}, because this datanode:{} is not in "
                    + "domain:{}",
                archiveId, storagePoolToBeUpdate.getPoolId(), datanodeId, domain.getDomainId());
            throw new ArchiveNotFoundExceptionThrift();
          }
          List<Long> archiveIds = datanodeMapArchives.get(datanodeId);
          RawArchiveMetadata archive = datanode.getArchiveById(archiveId);
          if (archive == null) {
            logger.error("can't find archive by given id:{}, datanode:{}", archiveId, datanode);
            throw new ArchiveNotFoundExceptionThrift();
          } else {
            if (archive.getStoragePoolId() != PoolInfo.AVAILABLE_POOLID) {
              throw new ArchiveNotFreeToUseExceptionThrift();
            } else {
              archiveIds.add(archiveId);
              // mark archive is in use
              archive.setStoragePoolId(storagePoolToBeUpdate.getPoolId());
            }
          }

          storageStore.save(datanode);
          storagePoolToBeUpdate.setLastUpdateTime(System.currentTimeMillis());
        }
      }

      for (Entry<Long, Collection<Long>> entry : storagePoolExist.getArchivesInDataNode().asMap()
          .entrySet()) {
        storagePoolToBeUpdate.getArchivesInDataNode().putAll(entry.getKey(), entry.getValue());
      }

      Map<Long, InstanceMetadata> instanceId2InstanceMetadata = new HashMap<>();
      Map<Long, RawArchiveMetadata> archiveId2Archive = new HashMap<>();
      for (InstanceMetadata instance : storageStore.list()) {
        if (instance.getDatanodeStatus().equals(OK)) {
          instanceId2InstanceMetadata.put(instance.getInstanceId().getId(), instance);
          for (RawArchiveMetadata archiveMetadata : instance.getArchives()) {
            archiveId2Archive.put(archiveMetadata.getArchiveId(), archiveMetadata);
          }
        }
      }
      long logicalSpace = 0;
      long logicalFreeSpace = 0;
      for (Long archiveId : storagePoolExist.getArchivesInDataNode().values()) {
        RawArchiveMetadata archiveMetadata = archiveId2Archive.get(archiveId);
        if (archiveMetadata != null) {
          logicalSpace += archiveMetadata.getLogicalSpace();
          logicalFreeSpace += archiveMetadata.getLogicalFreeSpace();
        }
      }

      storagePoolExist.setTotalSpace(logicalSpace);
      storagePoolExist.setFreeSpace(logicalFreeSpace);
      storagePoolToBeUpdate.setLogicalPssaaFreeSpace(StoragePoolSpaceCalculator
          .calculateFreeSpace(storagePoolToBeUpdate, instanceId2InstanceMetadata, archiveId2Archive,
              VolumeType.LARGE.getNumMembers(), segmentSize));
      storagePoolToBeUpdate.setLogicalPssFreeSpace(StoragePoolSpaceCalculator
          .calculateFreeSpace(storagePoolToBeUpdate, instanceId2InstanceMetadata, archiveId2Archive,
              VolumeType.REGULAR.getNumMembers(), segmentSize));
      storagePoolToBeUpdate.setLogicalPsaFreeSpace(StoragePoolSpaceCalculator
          .calculateFreeSpace(storagePoolToBeUpdate, instanceId2InstanceMetadata, archiveId2Archive,
              VolumeType.SMALL.getNumMembers(), segmentSize));

      // now we can save to storage pool store
      storagePoolToBeUpdate.setVolumeIds(storagePoolExist.getVolumeIds());
      storagePoolToBeUpdate.setStoragePoolLevel(storagePoolExist.getStoragePoolLevel());
      storagePoolToBeUpdate.setMigrationRuleId(storagePoolExist.getMigrationRuleId());
      storagePoolToBeUpdate.setDomainName(domain.getDomainName());
      storagePoolStore.saveStoragePool(storagePoolToBeUpdate);

      backupDbManager.backupDatabase();
      response.setRequestId(request.getRequestId());
      response.setDatanodeMapAddedArchives(datanodeMapArchives);

      securityManager
          .updateResourceName(request.getStoragePool().getPoolId(),
              request.getStoragePool().getPoolName());

      // notify datanode set archives storage pool id
      for (Entry<Long, List<Long>> entry : response.getDatanodeMapAddedArchives().entrySet()) {
        Long datanodeId = entry.getKey();
        List<Long> archiveIdList = entry.getValue();
        EndPoint endPoint = null;
        SetArchiveStoragePoolRequestThrift setStoragePoolRequest = null;
        try {
          setStoragePoolRequest = new SetArchiveStoragePoolRequestThrift();
          setStoragePoolRequest.setRequestId(RequestIdBuilder.get());
          Map<Long, Long> archiveIdMapStoragePoolId = new HashMap<>();
          for (Long archiveId : archiveIdList) {
            archiveIdMapStoragePoolId.put(archiveId, request.getStoragePool().getPoolId());
          }
          setStoragePoolRequest.setArchiveIdMapStoragePoolId(archiveIdMapStoragePoolId);

          endPoint = instanceStore.get(new InstanceId(datanodeId)).getEndPoint();
          DataNodeService.Iface dataNodeClient = dataNodeClientFactory
              .generateSyncClient(endPoint, timeout);
          logger.warn("gonna to send request to datanode:{}", endPoint);
          logger.warn("dataNodeClient.setArchiveStoragePool setStoragePoolRequest: {}",
              setStoragePoolRequest);
          dataNodeClient.setArchiveStoragePool(setStoragePoolRequest);
        } catch (ServiceHavingBeenShutdownThrift | GenericThriftClientFactoryException
            | InvalidInputExceptionThrift e) {
          logger.error("request: {} to datanode: {} failed", setStoragePoolRequest, endPoint, e);
          continue;
        }
      }
    } catch (EndPointNotFoundException e) {
      logger.error("Caught an exception", e);
      throw new EndPointNotFoundExceptionThrift().setDetail(e.getMessage());
    } catch (TooManyEndPointFoundException e) {
      logger.error("Caught an exception", e);
      throw new TooManyEndPointFoundExceptionThrift().setDetail(e.getMessage());
    } catch (GenericThriftClientFactoryException e) {
      logger.error("Caught an exception", e);
      throw new NetworkErrorExceptionThrift().setDetail(e.getMessage());
    } catch (TException e) {
      logger.error("Caught an exception", e);
      throw filterTexception(e);
    } catch (Exception e) {
      logger.error("caught an exception", e);
      throw new TException(e);
    }

    buildEndOperationWrtModifyStoragePoolAndSaveToDb(request.getAccountId(),
        request.getStoragePool().getPoolName());
    logger.warn("updateStoragePool response: {}", response);
    return response;
  }

  @Override
  public DeleteStoragePoolResponseThrift deleteStoragePool(DeleteStoragePoolRequestThrift request)
      throws ServiceHavingBeenShutdownThrift, InvalidInputExceptionThrift,
      StoragePoolNotExistedExceptionThrift, ServiceIsNotAvailableThrift,
      StillHaveVolumeExceptionThrift,
      DomainNotExistedExceptionThrift, StoragePoolIsDeletingExceptionThrift,
      ResourceNotExistsExceptionThrift,
      PermissionNotGrantExceptionThrift, AccountNotFoundExceptionThrift,
      AccessDeniedExceptionThrift,
      TException {
    //check the instance status
    checkInstanceStatus("deleteStoragePool");
    logger.warn("deleteStoragePool request: {}", request);
    securityManager.hasPermission(request.getAccountId(), "deleteStoragePool");
    securityManager.hasRightToAccess(request.getAccountId(), request.getDomainId());
    securityManager.hasRightToAccess(request.getAccountId(), request.getStoragePoolId());

    DeleteStoragePoolResponseThrift response = new DeleteStoragePoolResponseThrift();
    try {

      try {
        ValidateParam.validateDeleteStoragePoolRequest(request);
      } catch (InvalidInputExceptionThrift e) {
        throw new InvalidInputExceptionThrift();
      }
      // check domain id exist
      Domain domain = null;
      try {
        domain = domainStore.getDomain(request.getDomainId());
      } catch (Exception e) {
        logger.warn("can not get domain", e);
      }
      if (domain == null) {
        logger.warn("can't find any domain by id:{}", request.getDomainId());
        throw new DomainNotExistedExceptionThrift();
      }
      // check pool id exist
      StoragePool storagePoolExist = null;
      try {
        storagePoolExist = storagePoolStore.getStoragePool(request.getStoragePoolId());
      } catch (Exception e) {
        logger.warn("can not get storage pool", e);
      }
      if (storagePoolExist == null) {
        logger.error("storage pool is not exist any more");
        throw new StoragePoolNotExistedExceptionThrift();
      }

      if (storagePoolExist.getStatus() == Status.Deleting) {
        logger.error("storage pool is deleting, storage pool:{}", storagePoolExist);
        throw new StoragePoolIsDeletingExceptionThrift();
      }
      for (Long volumeId : storagePoolExist.getVolumeIds()) {
        VolumeMetadata volume = volumeStore.getVolume(volumeId);
        if (volume != null && volume.getVolumeStatus() != VolumeStatus.Dead) {
          if (volume.getName().contains(MOVE_VOLUME_APPEND_STRING_FOR_ORIGINAL_VOLUME)
              && (volume.getVolumeStatus() == VolumeStatus.Deleting
              || volume.getVolumeStatus() == VolumeStatus.Deleted)) {
            logger
                .warn("in deleteStoragePool, has volume:{}, which is moved, not care it", volumeId,
                    request.getStoragePoolId(), request.getDomainId());
            continue;
          }
          logger.error("still has volume:{} not dead in storage pool:{}, domain:{}", volumeId,
              request.getStoragePoolId(), request.getDomainId());
          throw new StillHaveVolumeExceptionThrift();
        }
      }
      // free all archives in storage pool
      for (Entry<Long, Long> entry : storagePoolExist.getArchivesInDataNode().entries()) {
        Long datanodeId = entry.getKey();
        Long archiveId = entry.getValue();
        InstanceMetadata datanode = storageStore.get(datanodeId);
        if (datanode == null) {
          // for now, datanode is not here, should wait it report and process it
          logger.warn("can't find datanode by given id:{}, storage:{}", datanodeId,
              storageStore.list());
          continue;
        }
        RawArchiveMetadata archive = datanode.getArchiveById(archiveId);
        if (archive != null) {
          archive.setFree();
        }

        storageStore.save(datanode);
      }

      // now can delete storage pool
      storagePoolExist.setLastUpdateTime(System.currentTimeMillis());
      storagePoolExist.setStatus(Status.Deleting);
      storagePoolStore.saveStoragePool(storagePoolExist);
     
      Map<Long, List<Long>> mapDatanodeToArchiveIds = new HashMap<>();
      for (Entry<Long, Collection<Long>> entry : storagePoolExist.getArchivesInDataNode().asMap()
          .entrySet()) {
        List<Long> datanodeIds = new ArrayList<>();
        datanodeIds.addAll(entry.getValue());
        mapDatanodeToArchiveIds.put(entry.getKey(), datanodeIds);
      }
      // response to control center to free all archives in datanode
      response.setDatanodeMapRemovedArchiveIds((mapDatanodeToArchiveIds));
      // now can delete storage pool in volume
      domain = null;
      try {
        domain = domainStore.getDomain(request.getDomainId());
      } catch (Exception e) {
        logger.error("get domain failed", e);
        throw new DomainNotExistedExceptionThrift();
      }
      domain.deleteStoragePool(request.getStoragePoolId());
      domainStore.saveDomain(domain);
      response.setRequestId(request.getRequestId());
      response.setStoragePoolName(storagePoolExist.getName());

      // handle with archives, should notify each datanode to free its'
      // archives
      if (response.isSetDatanodeMapRemovedArchiveIds() && !response
          .getDatanodeMapRemovedArchiveIds().isEmpty()) {
        for (Entry<Long, List<Long>> entry : response.getDatanodeMapRemovedArchiveIds()
            .entrySet()) {
          if (entry == null || entry.getKey() == null || entry.getValue() == null || entry
              .getValue()
              .isEmpty()) {
            // got nothing to notice, continue
            continue;
          }
          Long datanodeId = entry.getKey();
          List<Long> archiveIdList = entry.getValue();
          EndPoint endPoint = null;
          FreeArchiveStoragePoolRequestThrift freeArchiveRequest = null;
          try {
            Instance datanode = instanceStore.get(new InstanceId(datanodeId));
            if (datanode == null) {
              logger.warn("can not get datanode:{}, so we can not notice it", datanodeId);
              continue;
            }
            endPoint = datanode.getEndPoint();

            final DataNodeService.Iface dataNodeClient = dataNodeClientFactory
                .generateSyncClient(endPoint, timeout);
            freeArchiveRequest = new FreeArchiveStoragePoolRequestThrift();
            freeArchiveRequest.setRequestId(RequestIdBuilder.get());
            freeArchiveRequest.setFreeArchiveList(archiveIdList);
            logger.warn("gonna to send request to datanode:{}", endPoint);
            logger.warn("dataNodeClient.freeArchiveStoragePool freeArchiveRequest: {}",
                freeArchiveRequest);
            dataNodeClient.freeArchiveStoragePool(freeArchiveRequest);
          } catch (ServiceHavingBeenShutdownThrift | GenericThriftClientFactoryException
              | InvalidInputExceptionThrift e) {
            logger.error("request: {} to datanode: {} failed ", freeArchiveRequest, endPoint, e);
            continue;
          }
        }
      }
    } catch (TException e) {
      logger.error("Caught an exception", e);
      throw filterTexception(e);
    } catch (Exception e) {
      logger.error("caught an exception", e);
      throw new TException(e);
    }
    buildOperationAndSaveToDb(request.getAccountId(), request.getStoragePoolId(),
        OperationType.DELETE,
        TargetType.STORAGEPOOL, response.getStoragePoolName(), "", OperationStatus.ACTIVITING, 0L,
        null, null);
    logger.warn("deleteStoragePool response: {}", response);
    return response;
  }

  /******** StoragePool end.    *************/

  @Override
  public ListStoragePoolResponseThrift listStoragePools(ListStoragePoolRequestThrift request)
      throws ServiceHavingBeenShutdownThrift, InvalidInputExceptionThrift,
      ServiceIsNotAvailableThrift,
      AccountNotFoundExceptionThrift, EndPointNotFoundExceptionThrift,
      ResourceNotExistsExceptionThrift,
      TException {
    //check the instance status
    checkInstanceStatus("listStoragePools");
    logger.warn("listStoragePools request: {}", request);

   

    Long accountId = request.getAccountId();
    final Set<Long> accessibleResource = securityManager
        .getAccessibleResourcesByType(accountId, PyResource.ResourceType.StoragePool);
    List<Long> newStoragePoolIds;
    if (request.isSetStoragePoolIds()) {
      newStoragePoolIds = request.getStoragePoolIds().stream()
          .filter(l -> accessibleResource.contains(l))
          .collect(Collectors.toList());
    } else {
      newStoragePoolIds = new ArrayList<>(accessibleResource);
    }

    request.setStoragePoolIds(newStoragePoolIds);

    ListStoragePoolResponseThrift response = new ListStoragePoolResponseThrift();
    try {
      // check if list some or all storage pool
      List<StoragePool> storagePoolList = new ArrayList<>();
      try {
        storagePoolList.addAll(storagePoolStore.listStoragePools(request.getStoragePoolIds()));
      } catch (Exception e) {
        logger.warn("list storage pools caught an exception", e);
      }

      //get instance disk info
      Map<String, Map<String, DiskInfo>> instanceIp2DiskInfoMap = getInstanceDiskInfo();

      List<OneStoragePoolDisplayThrift> storagePoolDisplayList = new ArrayList<>();
      for (StoragePool storagePool : storagePoolList) {
        long storagePoolMigrationSpeed = 0;
        long storagePoolTotalPageToMigrate = 0;
        long storagePoolAlreadyMigratedPage = 0;
        List<ArchiveMetadataThrift> archiveList = new ArrayList<>();
        for (Entry<Long, Collection<Long>> entry : storagePool.getArchivesInDataNode().asMap()
            .entrySet()) {
          Long datanodeId = entry.getKey();
          Collection<Long> archiveIdList = entry.getValue();
          InstanceMetadata datanode = storageStore.get(datanodeId);

          if (datanode != null && archiveIdList != null && !archiveIdList.isEmpty()) {
            EndPoint datanodeEndpoint = EndPoint.fromString(datanode.getEndpoint());
            // determine
            boolean networkSubhealthy = determinDataNodeIsNetworkSubhealthy(
                datanodeEndpoint.getHostName());

            for (Long archiveId : archiveIdList) {
              RawArchiveMetadata archive = datanode.getArchiveById(archiveId);
              if (archive != null) {
                if (OK == datanode.getDatanodeStatus()) {
                  storagePoolMigrationSpeed += archive.getMigrationSpeed();
                  storagePoolTotalPageToMigrate += archive.getTotalPageToMigrate();
                  storagePoolAlreadyMigratedPage += archive.getAlreadyMigratedPage();
                }

                ArchiveMetadataThrift archiveMetadataThrift = RequestResponseHelper
                    .buildThriftArchiveMetadataFrom(archive);

               
                if (instanceIp2DiskInfoMap.containsKey(datanode.getEndpoint())) {
                  Map<String, DiskInfo> diskName2DiskInfoMap = instanceIp2DiskInfoMap
                      .get(datanode.getEndpoint());
                  if (diskName2DiskInfoMap.containsKey(archive.getDeviceName())) {
                    archiveMetadataThrift
                        .setRate(diskName2DiskInfoMap.get(archive.getDeviceName()).getRate());
                  }
                }
                if (networkSubhealthy) {
                  archiveMetadataThrift.setStatus(ArchiveStatusThrift.SEPARATED);
                }

                archiveList.add(archiveMetadataThrift);
              } else {
                logger.info("can not find archive info by archive Id:{}, at datanode id:{}",
                    archiveId,
                    datanodeId);
              }
            }
          } else {
            logger.warn("can not find datanode info by datanode Id:{}", datanodeId);
          }
        }
        storagePool.setMigrationSpeed(storagePoolMigrationSpeed);
        double storagePoolMigrationRatio = (0 == storagePoolTotalPageToMigrate)
            ? 100 : (storagePoolAlreadyMigratedPage * 100) / storagePoolTotalPageToMigrate;
        long totalMigrateDataSizeMb = 0;
        if (storagePoolTotalPageToMigrate != 0) {
          totalMigrateDataSizeMb = (storagePoolTotalPageToMigrate * pageSize) / mbSize;
        }
        logger.warn("going to mark pool:{} migrate ratio:{}, total migrate data size:{}",
            storagePool.getName(),
            storagePoolMigrationRatio, totalMigrateDataSizeMb);
        storagePool.setMigrationRatio(storagePoolMigrationRatio);
        storagePool.setTotalMigrateDataSizeMb(totalMigrateDataSizeMb);
       
       
        OneStoragePoolDisplayThrift storagePoolThrift = new OneStoragePoolDisplayThrift();
        MigrationRule migrationRule = null;
        if (storagePool.getMigrationRuleId() != StoragePool.DEFAULT_MIGRATION_RULE_ID.longValue()) {
          MigrationRuleInformation migrationRuleInformation = migrationRuleStore
              .get(storagePool.getMigrationRuleId());
          if (migrationRuleInformation != null) {
            migrationRule = migrationRuleInformation.toMigrationRule();
          }
        }
        storagePoolThrift.setStoragePoolThrift(
            RequestResponseHelper.buildThriftStoragePoolFrom(storagePool, migrationRule));
        storagePoolThrift.setArchiveThrifts(archiveList);
        storagePoolDisplayList.add(storagePoolThrift);
      }

      response.setStoragePoolDisplays(storagePoolDisplayList);
      response.setRequestId(request.getRequestId());

      List<OneStoragePoolDisplayThrift> storagePoolDisplayThrifts = response
          .getStoragePoolDisplays();
      // this is for delete the resource after storage pool is deleted and removed from
      // storagePoolStore
      for (Long poolId : newStoragePoolIds) {
        boolean found = false;
        for (OneStoragePoolDisplayThrift storagePoolDisplayThrift : storagePoolDisplayThrifts) {
          if (storagePoolDisplayThrift.getStoragePoolThrift().getPoolId() == poolId) {
            found = true;
            break;
          }
        }
        if (!found) {
          securityManager.unbindResource(poolId);
          securityManager.removeResource(poolId);
        }
      }

      // infocenter will return storagePool not in the request domain, remove it here.
      if (request.isSetDomainId()) {
        storagePoolDisplayThrifts.removeIf(
            storagePool -> storagePool.getStoragePoolThrift().getDomainId() != request
                .getDomainId());
      }

    } catch (TException e) {
      logger.error("Caught an exception", e);
      throw filterTexception(e);
    } catch (Exception e) {
      logger.error("caught an exception", e);
      throw new TException(e);
    }
    logger.warn("listStoragePools response: {}", response);
    return response;
  }

  @Override
  public RemoveArchiveFromStoragePoolResponseThrift removeArchiveFromStoragePool(
      RemoveArchiveFromStoragePoolRequestThrift request)
      throws ServiceHavingBeenShutdownThrift, InvalidInputExceptionThrift,
      ServiceIsNotAvailableThrift,
      FailToRemoveArchiveFromStoragePoolExceptionThrift, ArchiveNotFoundExceptionThrift,
      StoragePoolNotExistedExceptionThrift, DomainNotExistedExceptionThrift,
      StoragePoolIsDeletingExceptionThrift, PermissionNotGrantExceptionThrift,
      AccountNotFoundExceptionThrift,
      AccessDeniedExceptionThrift, TException {
    //check the instance status
    checkInstanceStatus("removeArchiveFromStoragePool");
    logger.warn("removeArchiveFromStoragePool request: {}", request);

    securityManager.hasPermission(request.getAccountId(), "removeArchiveFromStoragePool");
    securityManager.hasRightToAccess(request.getAccountId(), request.getStoragePoolId());

    RemoveArchiveFromStoragePoolResponseThrift response =
        new RemoveArchiveFromStoragePoolResponseThrift();
    try {

      try {
        ValidateParam.validateRemoveArchiveFromStoragePoolRequest(request);
      } catch (InvalidInputExceptionThrift e) {
        throw e;
      }
      // check domain id exist
      Domain domain = null;
      try {
        domain = domainStore.getDomain(request.getDomainId());
      } catch (Exception e) {
        logger.warn("can not get domain", e);
      }
      if (domain == null) {
        logger.warn("can't find any domain by id:{}", request.getDomainId());
        throw new DomainNotExistedExceptionThrift();
      }
      // check pool id exist
      StoragePool storagePoolExist = null;
      try {
        storagePoolExist = storagePoolStore.getStoragePool(request.getStoragePoolId());
      } catch (Exception e) {
        logger.warn("can not get storage pool", e);
      }
      if (storagePoolExist == null) {
        throw new StoragePoolNotExistedExceptionThrift();
      }

      if (storagePoolExist.getStatus() == Status.Deleting) {
        logger.error("storage pool is deleting, storage pool:{}", storagePoolExist);
        throw new StoragePoolIsDeletingExceptionThrift();
      }

      InstanceMetadata datanode = storageStore.get(request.getDatanodeInstanceId());
      if (datanode != null) {
        RawArchiveMetadata archive = datanode.getArchiveById(request.getArchiveId());
        if (archive != null) {
          // mark archive is free
          archive.setFree();
          storageStore.save(datanode);
        } else {
          logger.warn("can not find archive by id:{} in datanode:{}", request.getArchiveId(),
              request.getDatanodeInstanceId());
        }
      } else {
        logger.warn("can not find datanode by id:{}", request.getDatanodeInstanceId());
      }

      // now remove archive id from storage pool
      storagePoolExist
          .removeArchiveFromDatanode(request.getDatanodeInstanceId(), request.getArchiveId());
      storagePoolExist.setLastUpdateTime(System.currentTimeMillis());

      Map<Long, InstanceMetadata> instanceId2InstanceMetadata = new HashMap<>();
      Map<Long, RawArchiveMetadata> archiveId2Archive = new HashMap<>();
      for (InstanceMetadata instance : storageStore.list()) {
        if (instance.getDatanodeStatus().equals(OK)) {
          instanceId2InstanceMetadata.put(instance.getInstanceId().getId(), instance);
          for (RawArchiveMetadata archiveMetadata : instance.getArchives()) {
            archiveId2Archive.put(archiveMetadata.getArchiveId(), archiveMetadata);
          }
        }
      }

      long logicalSpace = 0;
      long logicalFreeSpace = 0;
      for (Long archiveId : storagePoolExist.getArchivesInDataNode().values()) {
        RawArchiveMetadata archiveMetadata = archiveId2Archive.get(archiveId);
        if (archiveMetadata != null) {
          logicalSpace += archiveMetadata.getLogicalSpace();
          logicalFreeSpace += archiveMetadata.getLogicalFreeSpace();
        }
      }

      storagePoolExist.setTotalSpace(logicalSpace);
      storagePoolExist.setFreeSpace(logicalFreeSpace);
      storagePoolExist.setLogicalPssaaFreeSpace(StoragePoolSpaceCalculator
          .calculateFreeSpace(storagePoolExist, instanceId2InstanceMetadata, archiveId2Archive,
              VolumeType.LARGE.getNumMembers(), segmentSize));
      storagePoolExist.setLogicalPssFreeSpace(StoragePoolSpaceCalculator
          .calculateFreeSpace(storagePoolExist, instanceId2InstanceMetadata, archiveId2Archive,
              VolumeType.REGULAR.getNumMembers(), segmentSize));
      storagePoolExist.setLogicalPsaFreeSpace(StoragePoolSpaceCalculator
          .calculateFreeSpace(storagePoolExist, instanceId2InstanceMetadata, archiveId2Archive,
              VolumeType.SMALL.getNumMembers(), segmentSize));

      storagePoolStore.saveStoragePool(storagePoolExist);
      backupDbManager.backupDatabase();
      response.setRequestId(request.getRequestId());
      response.setStoragePoolName(storagePoolExist.getName());

      // notify datanode to free archive
      FreeArchiveStoragePoolRequestThrift freeArchiveRequest = null;
      EndPoint endPoint = instanceStore.get(new InstanceId(request.getDatanodeInstanceId()))
          .getEndPoint();
      int tryTime = 3;
      for (int i = 0; i < tryTime; i++) {
        try {
          final DataNodeService.Iface dataNodeClient = dataNodeClientFactory
              .generateSyncClient(endPoint, timeout);
          freeArchiveRequest = new FreeArchiveStoragePoolRequestThrift();
          freeArchiveRequest.setRequestId(RequestIdBuilder.get());
          List<Long> archiveIdList = new ArrayList<>();
          archiveIdList.add(request.getArchiveId());
          freeArchiveRequest.setFreeArchiveList(archiveIdList);
          logger.warn("gonna to send request to datanode:{}", endPoint);
          dataNodeClient.freeArchiveStoragePool(freeArchiveRequest);
          break;
        } catch (ServiceHavingBeenShutdownThrift e) {
          logger.error("request:{} to datanode:{} failed ", freeArchiveRequest, endPoint, e);
          // do not throw cause when datanode startup, infocenter will release archive from
          // storage pool
        } catch (TTransportException | GenericThriftClientFactoryException e) {
          logger.warn("request:{} can not reach datanode:{}", freeArchiveRequest, endPoint, e);
          continue;
        }
      }
    } catch (InvalidInputExceptionThrift e) {
      logger.error("Caught an exception", e);
      throw e;
    } catch (TException e) {
      logger.error("Caught an exception", e);
      throw filterTexception(e);
    } catch (Exception e) {
      logger.error("caught an exception", e);
      throw new TException(e);
    }

    buildEndOperationWrtModifyStoragePoolAndSaveToDb(request.getAccountId(),
        response.getStoragePoolName());
    logger.warn("removeArchiveFromStoragePool response: {}", response);
    return response;
  }

  /******** ServerNode begin .   *************/
  @Override
  public ReportServerNodeInfoResponseThrift reportServerNodeInfo(
      ReportServerNodeInfoRequestThrift request)
      throws ServiceIsNotAvailableThrift, ServiceHavingBeenShutdownThrift, TException {

    //check the instance status
    checkInstanceStatus("reportServerNodeInfo");
    logger.info("begin reportServerNodeInfo, request is: {}", request);

    ServerNode serverNode = new ServerNode();
    serverNode.setId(request.getServerId());
    serverNode.setCpuInfo(request.getCpuInfo());
    serverNode.setDiskInfo(request.getDiskInfo());
    serverNode.setMemoryInfo(request.getMemoryInfo());
    serverNode.setModelInfo(request.getModelInfo());
    serverNode.setNetworkCardInfo(request.getNetworkCardInfo());
    serverNode.setNetworkCardInfoName(request.getNetworkCardInfoName());
    serverNode.setGatewayIp(request.getGatewayIp());
    serverNode.setHostName(request.getHostName());
    serverNode.setManageIp(request.getManageIp());
    serverNode.setStatus("ok");
    Set<SensorInfo> sensorInfos = new HashSet<>();
    for (SensorInfoThrift sensorInfoThrift : request.getSensorInfos()) {
      sensorInfos.add(buildSensorInfoFrom(sensorInfoThrift));
    }
    try {
      String sensorInfoBuf = serverNode.sensorInfoSet2String(sensorInfos);
      serverNode.setSensorInfo(diskInfoStore.createBlob(sensorInfoBuf.getBytes()));
    } catch (IOException e) {
      logger.error("get sensorInfoBuf error", e);
    }

    Set<HardDiskInfoThrift> hardDisks = request.getHardDisks();
    Set<DiskInfo> diskInfoSet = new HashSet<>();
    for (HardDiskInfoThrift diskInfoThrift : hardDisks) {
      if ("".equals(diskInfoThrift.getSn())) {
        logger.warn("disk sn is empty string, can not build disk primary key, serverNode id is: {}",
            serverNode.getId());
        continue;
      }
      String diskId = serverNode.getId() + "-" + diskInfoThrift.getName();
      DiskInfo diskInfo = diskInfoStore.listDiskInfoById(diskId);
      if (diskInfo == null) {
        diskInfo = new DiskInfo();
        diskInfo.setId(diskId);
      }
      diskInfo.setSn(diskInfoThrift.getSn());
      diskInfo.setName(diskInfoThrift.getName());
      diskInfo.setSsdOrHdd(diskInfoThrift.getSsdOrHdd());
      diskInfo.setVendor(diskInfoThrift.getVendor());
      diskInfo.setModel(diskInfoThrift.getModel());
      diskInfo.setRate(diskInfoThrift.getRate());
      diskInfo.setSize(diskInfoThrift.getSize());
      diskInfo.setWwn(diskInfoThrift.getWwn());
      diskInfo.setControllerId(diskInfoThrift.getControllerId());
      diskInfo.setSlotNumber(diskInfoThrift.getSlotNumber());
      diskInfo.setEnclosureId(diskInfoThrift.getEnclosureId());
      diskInfo.setCardType(diskInfoThrift.getCardType());
     
      diskInfo.setSerialNumber(diskInfoThrift.getSerialNumber());

      try {
        List<DiskSmartInfo> smartInfoList = new LinkedList<>();
        for (DiskSmartInfoThrift smartInfoThrift : diskInfoThrift.getSmartInfo()) {
          smartInfoList.add(buildDiskSmartInfoFrom(smartInfoThrift));
        }

        String smartInfoBuf = diskInfo.smartInfoList2String(smartInfoList);
        diskInfo.setSmartInfo(diskInfoStore.createBlob(smartInfoBuf.getBytes()));
      } catch (IOException e) {
        logger.warn("disk smart info convert from thrift to db failed. cause exception, ", e);
      }

      diskInfoSet.add(diskInfo);
    }

    serverNode.setDiskInfoSet(diskInfoSet);

    ServerNode serverNodeStored = serverNodeStore.listServerNodeById(request.getServerId());
    if (serverNodeStored != null) {
      serverNode.setStoreIp(serverNodeStored.getStoreIp());
      serverNode.setRackNo(serverNodeStored.getRackNo());
      serverNode.setSlotNo(serverNodeStored.getSlotNo());
      serverNode.setChildFramNo(serverNodeStored.getChildFramNo());
    }
    logger.warn("servernode is {}", serverNode);
    serverNodeStore.saveOrUpdateServerNode(serverNode);
    ReportServerNodeInfoResponseThrift response = new ReportServerNodeInfoResponseThrift();
    response.setResponseId(request.getRequestId());
    return response;
  }

  @Override
  public DeleteServerNodesResponseThrift deleteServerNodes(DeleteServerNodesRequestThrift request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      ServerNodeIsUnknownThrift,
      AccountNotFoundExceptionThrift, PermissionNotGrantExceptionThrift {

    //check the instance status
    checkInstanceStatus("deleteServerNodes");
    logger.warn("begin deleteServerNode, request is: {}", request);
    securityManager.hasPermission(request.getAccountId(), "deleteServerNodes");

    List<String> serverIds = request.getServerIds();
    List<String> deletedHostnames = new ArrayList<>();
    for (String serverId : serverIds) {
      ServerNode serverNode = serverNodeStore.listServerNodeById(serverId);
      if ("ok".equals(serverNode.getStatus())) {
        throw new ServerNodeIsUnknownThrift();
      }
      deletedHostnames.add(serverNode.getHostName());
    }
    serverNodeStore.deleteServerNodes(serverIds);

    DeleteServerNodesResponseThrift response = new DeleteServerNodesResponseThrift();
    response.setResponseId(request.getRequestId());
    response.setDeletedServerNodeHostnames(deletedHostnames);

    buildOperationAndSaveToDb(request.getAccountId(), null, OperationType.DELETE,
        TargetType.SERVERNODE,
        response.getDeletedServerNodeHostnames().stream().collect(Collectors.joining(", ")), "",
        OperationStatus.SUCCESS, 0L, null, null);

    return response;
  }

  @Override
  public UpdateServerNodeResponseThrift updateServerNode(UpdateServerNodeRequestThrift request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      ServerNodePositionIsRepeatExceptionThrift, TException {

    //check the instance status
    checkInstanceStatus("updateServerNode");
    logger.warn("begin updateServerNode, request is: {}", request);
    securityManager.hasPermission(request.getAccountId(), "updateServerNode");

    ServerNode serverNode = serverNodeStore.listServerNodeById(request.getServerId());
    if (request.getCpuInfo() != null) {
      serverNode.setCpuInfo(request.getCpuInfo());
    }
    if (request.getDiskInfo() != null) {
      serverNode.setDiskInfo(request.getDiskInfo());
    }
    if (request.getMemoryInfo() != null) {
      serverNode.setMemoryInfo(request.getMemoryInfo());
    }
    if (request.getModelInfo() != null) {
      serverNode.setModelInfo(request.getModelInfo());
    }
    if (request.getNetworkCardInfo() != null) {
      serverNode.setNetworkCardInfo(request.getNetworkCardInfo());
      serverNode.setNetworkCardInfoName(request.getNetworkCardInfoName());
    }
    if (request.getGatewayIp() != null) {
      serverNode.setGatewayIp(request.getGatewayIp());
    }

    if (request.getManageIp() != null) {
      serverNode.setManageIp(request.getManageIp());
    }
    if (request.getStoreIp() != null) {
      serverNode.setStoreIp(request.getStoreIp());
    }
    if (request.getRackNo() != null) {
      serverNode.setRackNo(request.getRackNo());
    }
    if (request.getSlotNo() != null) {
      serverNode.setSlotNo(request.getSlotNo());
    }
    if (request.getChildFramNo() != null) {
      serverNode.setChildFramNo(request.getChildFramNo());
    }

    if (serverNode.getRackNo() != null && serverNode.getSlotNo() != null
        && serverNode.getChildFramNo() != null) {
      String checkInfo =
          serverNode.getRackNo() + serverNode.getSlotNo() + serverNode.getChildFramNo();
      List<ServerNode> serverNodeList = serverNodeStore.listAllServerNodes();
      for (ServerNode node : serverNodeList) {
        if (serverNode.getId().equals(node.getId())) {
          continue;
        }
        String checkInfoTemp = node.getRackNo() + node.getSlotNo() + node.getChildFramNo();
        if (checkInfo.equals(checkInfoTemp)) {
          logger.warn(
              "rackNo, slotNo and childFramNo is repeat, rackNo:{}, slotNo:{}, childFramNo:{}");
          throw new ServerNodePositionIsRepeatExceptionThrift();
        }
      }
    }

    serverNodeStore.updateServerNode(serverNode);

    buildOperationAndSaveToDb(request.getAccountId(), null, OperationType.MODIFY,
        TargetType.SERVERNODE,
        request.getHostname(), "", OperationStatus.SUCCESS, 0L, null, null);

    UpdateServerNodeResponseThrift response = new UpdateServerNodeResponseThrift();
    response.setResponseId(request.getRequestId());
    return response;
  }

  @Override
  public ListServerNodesResponseThrift listServerNodes(ListServerNodesRequestThrift request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {
    //check the instance status
    checkInstanceStatus("listServerNodes");
    logger.warn("begin listServerNodes, request is: {}", request);

    int limit = request.getLimit();
    int offset = request.getPage() * limit;
    String sortField = request.getSortField();
    String sortDirection = request.getSortDirection();
    String hostName = request.getHostName();
    String modelInfo = request.getModelInfo();
    String cpuInfo = request.getCpuInfo();
    String memoryInfo = request.getMemoryInfo();
    String diskInfo = request.getDiskInfo();
    String networkCardInfo = request.getNetworkCardInfo();
    String manageIp = request.getManageIp();
    String gatewayIp = request.getGatewayIp();
    String storeIp = request.getStoreIp();
    String rackNo = request.getRackNo();
    String slotNo = request.getSlotNo();

    List<ServerNode> serverNodeList = serverNodeStore
        .listServerNodes(offset, limit, sortField, sortDirection, hostName, modelInfo, cpuInfo,
            memoryInfo,
            diskInfo, networkCardInfo, manageIp, gatewayIp, storeIp, rackNo, slotNo);

    List<ServerNodeThrift> serverNodeThriftList = new ArrayList<>();
    for (ServerNode serverNode : serverNodeList) {
      ServerNodeThrift serverNodeThrift = RequestResponseHelper
          .buildThriftServerNodeFrom(serverNode);

      //check server node is net sub health
      if (isServerNodeNetSubHealth(serverNode)) {
        serverNodeThrift.setDatanodeStatus(DatanodeStatusThrift.SEPARATED);
      }

      serverNodeThriftList.add(serverNodeThrift);
    }
    ListServerNodesResponseThrift response = new ListServerNodesResponseThrift();
    response.setResponseId(request.getRequestId());
    response.setRecordsTotal(serverNodeStore.getCountTotle());
    response.setRecordsAfterFilter(serverNodeThriftList.size());
    response.setServerNodesList(serverNodeThriftList);

    logger.warn("list serverNodes, response is: {}", response);
    return response;
  }

  @Override
  public ListServerNodeByIdResponseThrift listServerNodeById(
      ListServerNodeByIdRequestThrift request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {
    logger.warn("begin listServerNodeById, request is:{}", request);
    ServerNode serverNode = serverNodeStore.listServerNodeById(request.getServerId());
    ListServerNodeByIdResponseThrift response = new ListServerNodeByIdResponseThrift();
    response.setResponseId(request.getRequestId());

    ServerNodeThrift serverNodeThrift = RequestResponseHelper
        .buildThriftServerNodeFrom(serverNode);
    //check server node is net sub health
    if (isServerNodeNetSubHealth(serverNode)) {
      serverNodeThrift.setDatanodeStatus(DatanodeStatusThrift.SEPARATED);
    }

    response.setServerNode(serverNodeThrift);
    logger.debug("response is {}", response);
    return response;
  }

  @Override
  public TurnOffAllDiskLightByServerIdResponseThrift turnOffAllDiskLightByServerId(
      TurnOffAllDiskLightByServerIdRequestThrift request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {

    //check the instance status
    checkInstanceStatus("turnOffAllDiskLightByServerId");
    logger.warn("turnOffAllDiskLightByServerId, request is: {}", request);

    ServerNode serverNode = serverNodeStore.listServerNodeById(request.getServerId());
    int count = 0;
    while (serverNode == null) {
      logger.warn("server id is not in database. server id is {}", request.getServerId());
      serverNode = serverNodeStore.listServerNodeById(request.getServerId());
      if (count++ == 10) {
        logger.warn("we have attempted 10 times, but did not get the server by server id.");
        return new TurnOffAllDiskLightByServerIdResponseThrift(request.getRequestId());
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        logger.warn("caught an exception, ", e);
      }
    }

    Set<DiskInfo> diskInfoSet = serverNode.getDiskInfoSet();
    for (DiskInfo diskInfo : diskInfoSet) {
      diskInfoStore
          .updateDiskInfoLightStatusById(diskInfo.getId(), DiskInfoLightStatus.OFF.toString());
    }
    logger.warn("end turnOffAllDiskLightByServerId.");
    return new TurnOffAllDiskLightByServerIdResponseThrift(request.getRequestId());
  }

  /******** ServerNode end .   *************/

  @Override
  public GetIoLimitationResponseThrift getIoLimitationsInOneDriverContainer(
      GetIoLimitationRequestThrift request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {

    //check the instance status
    checkInstanceStatus("getIoLimitationsInOneDriverContainer");
    logger.warn("getIoLimitationsInOneDriverContainer, request:{}", request);

    GetIoLimitationResponseThrift responseThrift = new GetIoLimitationResponseThrift();
    responseThrift.setRequestId(request.getRequestId());
    responseThrift.setDriverContainerId(request.getDriverContainerId());

    List<DriverMetadata> driverMetadataList = driverStore.list();

    Map<DriverKeyThrift, List<IoLimitationThrift>> mapDriver2ItsIoLimitations = new HashMap<>();
    for (DriverMetadata driverMetadata : driverMetadataList) {
      if (driverMetadata.getDriverContainerId() == request.getDriverContainerId()) {

        List<IoLimitationThrift> ioLimitationThriftList = new ArrayList<>();
        if (driverMetadata.getDynamicIoLimitationId() != 0) {
          IoLimitation ioLimitation = ioLimitationStore
              .get(driverMetadata.getDynamicIoLimitationId());
          ioLimitationThriftList
              .add(RequestResponseHelper.buildThriftIoLimitationFrom(ioLimitation));
        }

        if (driverMetadata.getStaticIoLimitationId() != 0) {
          IoLimitation ioLimitation = ioLimitationStore
              .get(driverMetadata.getStaticIoLimitationId());
          ioLimitationThriftList
              .add(RequestResponseHelper.buildThriftIoLimitationFrom(ioLimitation));
        }

        DriverKey driverKey = new DriverKey(driverMetadata.getDriverContainerId(),
            driverMetadata.getVolumeId(),
            driverMetadata.getSnapshotId(), driverMetadata.getDriverType());
        DriverKeyThrift driverKeyThrift = RequestResponseHelper
            .buildThriftDriverKeyFrom(driverKey);
        logger
            .warn("got driver:{} has io limitations:{}", driverKeyThrift, ioLimitationThriftList);
        mapDriver2ItsIoLimitations.put(driverKeyThrift, ioLimitationThriftList);
      }
    }

    responseThrift.setMapDriver2ItsIoLimitations(mapDriver2ItsIoLimitations);
    logger.warn("getIoLimitationsInOneDriverContainer, response:{}", responseThrift);
    return responseThrift;
  }

  @Override
  public GetServerNodeByIpResponse getServerNodeByIp(GetServerNodeByIpRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {

    //check the instance status
    checkInstanceStatus("getServerNodeByIp");
    logger.warn("getServerNodeByIp request is: {}", request);

    ServerNode serverNodeByIp = serverNodeStore.getServerNodeByIp(request.getIp());
    ServerNodeThrift serverNodeThrift = RequestResponseHelper
        .buildThriftServerNodeFrom(serverNodeByIp);

    GetServerNodeByIpResponse response = new GetServerNodeByIpResponse();
    response.setResponseId(request.getRequestId());
    response.setServerNode(serverNodeThrift);
    logger.warn("getServerNodeByIp response is: {}", response);
    return response;
  }

  /******** RebalanceRule begin.    *************/
  @Override
  public void pauseAutoRebalance() throws TException {
    logger.warn("disable rebalance!");
    segmentUnitsDistributionManager.setRebalanceEnable(false);
  }

  @Override
  public boolean rebalanceStarted() throws TException {
    return segmentUnitsDistributionManager.isRebalanceEnable();
  }

  @Override
  public void addRebalanceRule(AddRebalanceRuleRequest request)
      throws RebalanceRuleExistingExceptionThrift, ServiceHavingBeenShutdownThrift,
      ServiceIsNotAvailableThrift, PermissionNotGrantExceptionThrift,
      AccountNotFoundExceptionThrift {

    //check the instance status
    checkInstanceStatus("addRebalanceRule");
    logger.warn("addRebalanceRule request: {}", request);

    securityManager.hasPermission(request.getAccountId(), "addRebalanceRule");
    RebalanceRulethrift rebalanceRuleThrift = request.getRule();
    if (rebalanceRuleThrift == null) {
      logger.error("add rule is null");
      return;
    }
    RebalanceRule rebalanceRule = RequestResponseHelper
        .convertRebalanceRuleThrift2RebalanceRule(rebalanceRuleThrift);

    long ruleId = rebalanceRuleThrift.getRuleId();
    RebalanceRuleInformation dbRebalanceRuleInformation = rebalanceRuleStore.get(ruleId);
    if (dbRebalanceRuleInformation != null) {
      logger.error("rule:{} is already exists", ruleId);
      throw new RebalanceRuleExistingExceptionThrift();
    }
    List<RebalanceRuleInformation> rebalanceRuleInformationList = rebalanceRuleStore.list();
    for (RebalanceRuleInformation ruleInformation : rebalanceRuleInformationList) {
      if (rebalanceRuleThrift.getRuleName().equals(ruleInformation.getRuleName())) {
        logger.error("rule:{} is already exists", rebalanceRuleThrift.getRuleName());
        throw new RebalanceRuleExistingExceptionThrift();
      }
    }
    rebalanceRuleStore.save(rebalanceRule.toRebalanceRuleInformation());
    return;
  }

  @Override
  public void updateRebalanceRule(UpdateRebalanceRuleRequest request)
      throws RebalanceRuleExistingExceptionThrift, ServiceHavingBeenShutdownThrift,
      ServiceIsNotAvailableThrift, PermissionNotGrantExceptionThrift,
      AccountNotFoundExceptionThrift,
      RebalanceRuleNotExistExceptionThrift {

    //check the instance status
    checkInstanceStatus("updateRebalanceRule");
    logger.warn("updateRebalanceRule request: {}", request);

    try {
      securityManager.hasPermission(request.getAccountId(), "updateRebalanceRule");
    } catch (PermissionNotGrantExceptionThrift permissionNotGrantExceptionThrift) {
      throw new PermissionNotGrantExceptionThrift();
    } catch (AccountNotFoundExceptionThrift accountNotFoundExceptionThrift) {
      throw new AccountNotFoundExceptionThrift();
    }

    RebalanceRulethrift rebalanceRuleThrift = request.getRule();
    if (rebalanceRuleThrift == null) {
      logger.error("update rule is null");
      return;
    }
    RebalanceRule rebalanceRule = RequestResponseHelper
        .convertRebalanceRuleThrift2RebalanceRule(rebalanceRuleThrift);

    long ruleId = rebalanceRuleThrift.getRuleId();
    RebalanceRuleInformation dbRuleInformation = rebalanceRuleStore.get(ruleId);
    if (dbRuleInformation == null) {
      logger.error("rule:{} is not exists", ruleId);
      throw new RebalanceRuleNotExistExceptionThrift();
    }

    List<RebalanceRuleInformation> ruleInformationFromDb = rebalanceRuleStore.list();
    for (RebalanceRuleInformation ruleInfo : ruleInformationFromDb) {
      if (dbRuleInformation.getRuleId() != ruleInfo.getRuleId()
          && ruleInfo.getRuleName().equalsIgnoreCase(rebalanceRuleThrift.getRuleName())) {
        logger.error("rule:{} is already exists", ruleId);
        throw new RebalanceRuleExistingExceptionThrift();
      }

    }
    //update
    RebalanceRuleInformation newRebalanceRuleInformation = rebalanceRule
        .toRebalanceRuleInformation();
    if (rebalanceRuleThrift.isSetRuleName()) {
      dbRuleInformation.setRuleName(newRebalanceRuleInformation.getRuleName());
    }
    if (rebalanceRuleThrift.isSetRelativeTime()) {
      dbRuleInformation.setWaitTime(newRebalanceRuleInformation.getWaitTime());
    }
    if (rebalanceRuleThrift.isSetAbsoluteTimeList()) {
      dbRuleInformation.setAbsoluteTimeJson(newRebalanceRuleInformation.getAbsoluteTimeJson());
    }
    rebalanceRuleStore.update(dbRuleInformation);
  }

  @Override
  public GetRebalanceRuleResponse getRebalanceRule(GetRebalanceRuleRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      PermissionNotGrantExceptionThrift,
      AccountNotFoundExceptionThrift {

    //check the instance status
    checkInstanceStatus("getRebalanceRule");
    logger.warn("getRebalanceRule request:{}", request);

    try {
      securityManager.hasPermission(request.getAccountId(), "getRebalanceRule");
    } catch (PermissionNotGrantExceptionThrift permissionNotGrantExceptionThrift) {
      throw new PermissionNotGrantExceptionThrift();
    } catch (AccountNotFoundExceptionThrift accountNotFoundExceptionThrift) {
      throw new AccountNotFoundExceptionThrift();
    }

    GetRebalanceRuleResponse response = new GetRebalanceRuleResponse();
    response.setRequestId(request.getRequestId());

    List<RebalanceRulethrift> rebalanceRuleThriftList = new ArrayList<>();

    List<Long> requestRuleIds = request.getRuleIdList();
    if (requestRuleIds == null || requestRuleIds.isEmpty()) {
      List<RebalanceRuleInformation> rebalanceRuleList = rebalanceRuleStore.list();
      for (RebalanceRuleInformation rebalanceRule : rebalanceRuleList) {
        RebalanceRulethrift rebalanceRuleThrift = RequestResponseHelper
            .convertRebalanceRule2RebalanceRuleThrift(rebalanceRule.toRebalanceRule());
        if (rebalanceRuleThrift != null) {
          rebalanceRuleThriftList.add(rebalanceRuleThrift);
        }
      }

    } else {
      for (long ruleId : requestRuleIds) {
        RebalanceRuleInformation rebalanceRule = rebalanceRuleStore.get(ruleId);
        RebalanceRulethrift rebalanceRuleThrift = RequestResponseHelper
            .convertRebalanceRule2RebalanceRuleThrift(rebalanceRule.toRebalanceRule());
        if (rebalanceRuleThrift != null) {
          rebalanceRuleThriftList.add(rebalanceRuleThrift);
        }
      }
    }

    response.setRules(rebalanceRuleThriftList);
    logger.warn("get rebalance rule response:{}", response);
    return response;
  }

  @Override
  public DeleteRebalanceRuleResponse deleteRebalanceRule(DeleteRebalanceRuleRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      PermissionNotGrantExceptionThrift,
      AccountNotFoundExceptionThrift {

    //check the instance status
    checkInstanceStatus("deleteRebalanceRule");
    logger.warn("DeleteRebalanceRuleResponse request: {}", request);
    securityManager.hasPermission(request.getAccountId(), "deleteRebalanceRule");

    List<Long> delRuleIdList = request.getRuleIdList();

    DeleteRebalanceRuleResponse response = new DeleteRebalanceRuleResponse();
    response.setRequestId(request.getRequestId());
    List<RebalanceRulethrift> delFailedRuleList = new ArrayList<>();

    if (delRuleIdList == null || delRuleIdList.isEmpty()) {
      List<RebalanceRuleInformation> ruleList = rebalanceRuleStore.list();
      for (RebalanceRuleInformation rule : ruleList) {
        try {
          rebalanceRuleStore.delete(rule.getRuleId());
        } catch (Exception e) {
          logger.warn("delete rule:{} failed", rule);
          RebalanceRulethrift rebalanceRuleThrift = RequestResponseHelper
              .convertRebalanceRule2RebalanceRuleThrift(rule.toRebalanceRule());
          if (rebalanceRuleThrift != null) {
            delFailedRuleList.add(rebalanceRuleThrift);
          }
        }
      }
    } else {
      for (long delRuleId : delRuleIdList) {
        try {
          rebalanceRuleStore.delete(delRuleId);
        } catch (Exception e) {
          RebalanceRuleInformation rule = rebalanceRuleStore.get(delRuleId);
          if (rule != null) {
            logger.warn("delete rule:{} failed", rule);
            RebalanceRulethrift rebalanceRuleThrift = RequestResponseHelper
                .convertRebalanceRule2RebalanceRuleThrift(rule.toRebalanceRule());
            if (rebalanceRuleThrift != null) {
              delFailedRuleList.add(rebalanceRuleThrift);
            }
          }
        }
      }
    }

    response.setFailedRuleIdList(delFailedRuleList);
    return response;
  }

  @Override
  public GetAppliedRebalanceRulePoolResponse getAppliedRebalanceRulePool(
      GetAppliedRebalanceRulePoolRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      PermissionNotGrantExceptionThrift,
      AccountNotFoundExceptionThrift, RebalanceRuleNotExistExceptionThrift,
      StoragePoolNotExistedExceptionThrift {

    //check the instance status
    checkInstanceStatus("getAppliedRebalanceRulePool");
    logger.warn("getAppliedRebalanceRulePool request: {}", request);
    securityManager.hasPermission(request.getAccountId(), "getAppliedRebalanceRulePool");

    GetAppliedRebalanceRulePoolResponse response = new GetAppliedRebalanceRulePoolResponse();
    response.setRequestId(request.getRequestId());
    long ruleId = request.getRuleId();

    RebalanceRuleInformation rebalanceRuleInformation = rebalanceRuleStore.get(ruleId);
    if (rebalanceRuleInformation == null) {
      logger.error("get applied rule pool failed! rule:{} is not exists!", ruleId);
      throw new RebalanceRuleNotExistExceptionThrift();
    }

    List<StoragePoolThrift> storagePoolThriftList = new ArrayList<>();

    List<Long> poolIdList = RebalanceRuleInformation
        .poolIdStr2PoolIdList(rebalanceRuleInformation.getPoolIds());
    if (poolIdList.isEmpty()) {
      response.setStoragePoolList(storagePoolThriftList);
      logger.warn("getAppliedRebalanceRulePool response:{}", response);
      return response;
    }

    List<StoragePool> storagePoolList;
    try {
      storagePoolList = storagePoolStore.listAllStoragePools();
    } catch (SQLException | IOException e) {
      logger.error("get applied rule:{] pool failed! get pools from store failed!", ruleId);
      throw new StoragePoolNotExistedExceptionThrift();
    }

    for (long poolId : poolIdList) {
      for (StoragePool storagePool : storagePoolList) {
        if (storagePool.getPoolId() != poolId) {
          continue;
        }

        storagePoolThriftList
            .add(RequestResponseHelper.buildThriftStoragePoolFrom(storagePool, null));
      }
    }

    response.setStoragePoolList(storagePoolThriftList);
    logger.warn("getAppliedRebalanceRulePool response:{}", response);
    return response;
  }

  @Override
  public GetUnAppliedRebalanceRulePoolResponse getUnAppliedRebalanceRulePool(
      GetUnAppliedRebalanceRulePoolRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      PermissionNotGrantExceptionThrift,
      AccountNotFoundExceptionThrift, StoragePoolNotExistedExceptionThrift {

    //check the instance status
    checkInstanceStatus("getUnAppliedRebalanceRulePool");
    logger.warn("getUnAppliedRebalanceRulePool request: {}", request);
    securityManager.hasPermission(request.getAccountId(), "getUnAppliedRebalanceRulePool");

    GetUnAppliedRebalanceRulePoolResponse response = new GetUnAppliedRebalanceRulePoolResponse();
    response.setRequestId(request.getRequestId());

    Set<Long> appliedPoolIdSet = new HashSet<>();
    List<RebalanceRuleInformation> appliedRebalanceRuleInformationList = rebalanceRuleStore
        .getAppliedRules();
    if (appliedRebalanceRuleInformationList != null) {
      for (RebalanceRuleInformation appliedRuleInformation : appliedRebalanceRuleInformationList) {
        if (appliedRuleInformation == null) {
          continue;
        }

        List<Long> poolIdsOfThisRule = RebalanceRuleInformation
            .poolIdStr2PoolIdList(appliedRuleInformation.getPoolIds());
        appliedPoolIdSet.addAll(poolIdsOfThisRule);
      }
    }

    List<StoragePool> storagePoolList;
    try {
      storagePoolList = storagePoolStore.listAllStoragePools();
    } catch (SQLException | IOException e) {
      logger.error("get unapplied rule pool failed! get pools from store failed!");
      throw new StoragePoolNotExistedExceptionThrift();
    }

    List<StoragePoolThrift> storagePoolThriftList = new ArrayList<>();
    for (StoragePool storagePool : storagePoolList) {
      if (appliedPoolIdSet.contains(storagePool.getPoolId())) {
        continue;
      }

      storagePoolThriftList
          .add(RequestResponseHelper.buildThriftStoragePoolFrom(storagePool, null));
    }

    response.setStoragePoolList(storagePoolThriftList);
    logger.warn("getUnAppliedRebalanceRulePool response:{}", response);
    return response;
  }

  @Override
  public void applyRebalanceRule(ApplyRebalanceRuleRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      PermissionNotGrantExceptionThrift,
      AccountNotFoundExceptionThrift, StoragePoolNotExistedExceptionThrift,
      PoolAlreadyAppliedRebalanceRuleExceptionThrift, RebalanceRuleNotExistExceptionThrift {

    //check the instance status
    checkInstanceStatus("applyRebalanceRule");
    logger.warn("applyRebalanceRule request: {}", request);
    securityManager.hasPermission(request.getAccountId(), "applyRebalanceRule");

    RebalanceRulethrift rebalanceRuleThrift = request.getRule();
    if (rebalanceRuleThrift == null) {
      logger.warn("apply rebalance rule failed! request is null!");
      return;
    }

    long ruleId = rebalanceRuleThrift.getRuleId();
    RebalanceRuleInformation dbRule = rebalanceRuleStore.get(ruleId);
    if (dbRule == null) {
      logger.error("rule:{} is not exists", ruleId);
      throw new RebalanceRuleNotExistExceptionThrift();
    }

    List<Long> poolIdList = request.getStoragePoolIdList();
    if (poolIdList == null) {
      logger.warn("apply rebalance rule failed! poolId is null!");
      return;
    }

    List<StoragePool> allStoragePoolList;
    try {
      allStoragePoolList = storagePoolStore.listAllStoragePools();
    } catch (SQLException | IOException e) {
      logger.error("apply rebalance rule:{} failed! get storage pool failed!", ruleId, e);
      throw new StoragePoolNotExistedExceptionThrift();
    }

    for (long poolId : poolIdList) {
      boolean poolExists = false;
      for (StoragePool storagePool : allStoragePoolList) {
        if (poolId == storagePool.getPoolId()) {
          poolExists = true;
          break;
        }
      }
      if (!poolExists) {
        logger.error("apply rebalance rule:{} failed! storage pool:{} not exists!", ruleId, poolId);
        throw new StoragePoolNotExistedExceptionThrift();
      }

      //if pool is already applied with a rule
      RebalanceRuleInformation existRule = rebalanceRuleStore.getRuleOfPool(poolId);
      if (existRule != null && existRule.getRuleId() != dbRule.getRuleId()) {
        RebalanceRule rule = existRule.toRebalanceRule();
        logger
            .warn("apply rebalance rule:{} failed! pool:{} is already applied with rule:{}", ruleId,
                poolId,
                rule);
        PoolAlreadyAppliedRebalanceRuleExceptionThrift exceptionThrift =
            new PoolAlreadyAppliedRebalanceRuleExceptionThrift();
        exceptionThrift
            .setRebalanceRule(RequestResponseHelper.convertRebalanceRule2RebalanceRuleThrift(rule));
        throw exceptionThrift;
      }
    }

    RebalanceRule rebalanceRule = RequestResponseHelper
        .convertRebalanceRuleThrift2RebalanceRule(request.getRule());
    rebalanceRuleStore.applyRule(rebalanceRule.toRebalanceRuleInformation(), poolIdList);
  }

  @Override
  public void unApplyRebalanceRule(UnApplyRebalanceRuleRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      PermissionNotGrantExceptionThrift,
      AccountNotFoundExceptionThrift, StoragePoolNotExistedExceptionThrift,
      StoragePoolNotExistedExceptionThrift, RebalanceRuleNotExistExceptionThrift {

    //check the instance status
    checkInstanceStatus("unApplyRebalanceRule");
    logger.warn("unApplyRebalanceRule request: {}", request);
    securityManager.hasPermission(request.getAccountId(), "unApplyRebalanceRule");

    if (!request.isSetRule()) {
      logger.warn("apply rebalance rule failed! request is null!");
      return;
    }

    RebalanceRulethrift rebalanceRuleThrift = request.getRule();
    long ruleId = rebalanceRuleThrift.getRuleId();
    RebalanceRuleInformation dbRule = rebalanceRuleStore.get(ruleId);
    if (dbRule == null) {
      logger.error("rule:{} is not exists", ruleId);
      throw new RebalanceRuleNotExistExceptionThrift();
    }

    List<Long> poolIdList = request.getStoragePoolIdList();
    if (poolIdList == null) {
      logger.warn("apply rebalance rule failed! poolId is null!");
      return;
    }

    RebalanceRule rebalanceRule = RequestResponseHelper
        .convertRebalanceRuleThrift2RebalanceRule(request.getRule());
    rebalanceRuleStore.unApplyRule(rebalanceRule.toRebalanceRuleInformation(), poolIdList);
  }
  /* RebalanceRule end    *************/

  /******** PerformanceFromPyMetrics begin .    *************/

  @Deprecated
  @Override
  public RetrieveOneRebalanceTaskResponse retrieveOneRebalanceTask(
      RetrieveOneRebalanceTaskRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, NoNeedToRebalanceThrift,
      TException {
    if (shutDownFlag) {
      throw new ServiceHavingBeenShutdownThrift();
    }

    logger.info("retrieveARebalanceTask request: {}", request);

    try {
      RebalanceTask rebalanceTask = segmentUnitsDistributionManager
          .selectRebalanceTask(request.isRecord());
      RebalanceTaskThrift rebalanceTaskThrift = RequestResponseHelper
          .buildRebalanceTaskThrift(rebalanceTask);
      RetrieveOneRebalanceTaskResponse response = new RetrieveOneRebalanceTaskResponse();
      response.setRequestId(request.getRequestId());
      response.setRebalanceTask(rebalanceTaskThrift);
      logger.warn("retrieveARebalanceTask response: {}", response);
      return response;
    } catch (NoNeedToRebalance e) {
      throw new NoNeedToRebalanceThrift();
    }
  }

  @Override
  public boolean discardRebalanceTask(long requestTaskId)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {

    checkInstanceStatus("discardRebalanceTask");
    logger.warn("request to discard a rebalance task {}", requestTaskId);
    return segmentUnitsDistributionManager.discardRebalanceTask(requestTaskId);
  }

  /**
   * get the Iops and throughput of the selected volume in the past one hour.
   */
  @Override
  public GetPerformanceFromPyMetricsResponseThrift pullPerformanceFromPyMetrics(
      GetPerformanceParameterRequestThrift request)
      throws VolumeHasNotBeenLaunchedExceptionThrift,
      ReadPerformanceParameterFromFileExceptionThrift,
      ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {

    //check the instance status
    checkInstanceStatus("pullPerformanceFromPyMetrics");
    logger.warn("pullPerformanceFromPyMetrics request: {}", request);

    GetPerformanceFromPyMetricsResponseThrift returnResponse =
        new GetPerformanceFromPyMetricsResponseThrift();

    try {
      VolumeMetadata volumeMetadata = volumeStore.getVolume(request.getVolumeId());

      if (volumeMetadata == null) {
        logger.error("No volume found with id {}", request.getVolumeId());
        throw new VolumeNotFoundExceptionThrift();
      }

      List<DriverMetadata> driversList = driverStore.get(request.getVolumeId());
      Map<Long, Long> totalReadIops = new HashMap<>();
      Map<Long, Long> totalWriteIops = new HashMap<>();
      Map<Long, Long> totalReadThroughput = new HashMap<>();
      Map<Long, Long> totalWriteThroughput = new HashMap<>();

      for (DriverMetadata driverMetadata : driversList) {

        final DriverContainerServiceBlockingClientWrapper clientDt;
        GetPerformanceParameterRequestThrift driverRequest =
            new GetPerformanceParameterRequestThrift();

        driverRequest.setRequestId(request.getRequestId());
        driverRequest.setVolumeId(request.getVolumeId());
        // try get port of driverContainer
        int containerPort = 0;
        Set<Instance> instanceSet = getInstanceStore().getAll();

        for (Instance instance : instanceSet) {
          logger.debug("current instance {}", instance);
         
          if (instance.getName().contentEquals("DriverContainer")) {
            EndPoint endPoint = instance.getEndPoint();
            if (endPoint == null) {
              logger
                  .warn("you have not set the end point for main service on instance {}", endPoint);
            } else {
              containerPort = endPoint.getPort();
            }

            break;
          }
        }

        clientDt = driverContainerClientFactory
            .build(new EndPoint(driverMetadata.getHostName(), containerPort), 2000);

        logger.warn("driverRequest: {}", driverRequest);
        GetPerformanceFromPyMetricsResponseThrift response = clientDt
            .pullPerformanceFromPyMetrics(driverRequest);

        if (response != null) {
          mergePerformanceParameterMap(totalReadIops, response.getReadIoPs());
          mergePerformanceParameterMap(totalWriteIops, response.getWriteIoPs());
          mergePerformanceParameterMap(totalReadThroughput, response.getReadThroughput());
          mergePerformanceParameterMap(totalWriteThroughput, response.getWriteThroughput());
        }
      }
      returnResponse.setReadIoPs(totalReadIops);
      returnResponse.setWriteIoPs(totalWriteIops);
      returnResponse.setReadThroughput(totalReadThroughput);
      returnResponse.setWriteThroughput(totalWriteThroughput);
    } catch (Exception e) {
      logger.warn("can't get data", e);
    }
    logger.warn("pullPerformanceFromPyMetrics response: {}", returnResponse);
    return returnResponse;
  }
  /* PerformanceFromPyMetrics end     *************/

  /******** IoLimitatio begin.     *************/

  @Override
  public GetPerformanceResponseThrift pullPerformanceParameter(
      GetPerformanceParameterRequestThrift request)
      throws VolumeHasNotBeenLaunchedExceptionThrift,
      ReadPerformanceParameterFromFileExceptionThrift,
      ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {

    //check the instance status
    checkInstanceStatus("pullPerformanceParameter");
    logger.warn("pullPerformanceParameter request: {}", request);

    GetPerformanceResponseThrift returnResponse = new GetPerformanceResponseThrift();

    try {
      long volumeId = request.getVolumeId();
      VolumeMetadata volumeMetadata = volumeStore.getVolume(volumeId);

      if (volumeMetadata == null) {
        logger.error("No volume found with id {}", volumeId);
        throw new VolumeNotFoundExceptionThrift();
      }

      // get all driver bound to the volume
      List<DriverMetadata> driversList = driverStore.get(volumeId);
      if (driversList == null || driversList.isEmpty()) {
        logger.warn("volume:{} has not been launched", volumeId);
        return returnResponse;
      }

     
     
     
     
     

     
     
     
     
     
      py.common.struct.Pair<Integer, Long> readLatency = new py.common.struct.Pair<>(0, 0L);
      py.common.struct.Pair<Integer, Long> writeLatency = new py.common.struct.Pair<>(0, 0L);
      long readIops = 0;
      long writeIops = 0;
      long readThroughput = 0;
      long writeThroughput = 0;

      for (DriverMetadata driverMetadata : driversList) {

        DriverContainerServiceBlockingClientWrapper clientDt;
        GetPerformanceParameterRequestThrift getPerformanceRequestThrift =
            new GetPerformanceParameterRequestThrift();

        getPerformanceRequestThrift.setRequestId(request.getRequestId());
        getPerformanceRequestThrift.setVolumeId(request.getVolumeId());
        getPerformanceRequestThrift.setSnapshotId(driverMetadata.getSnapshotId());
        getPerformanceRequestThrift
            .setDriverType(DriverTypeThrift.valueOf(driverMetadata.getDriverType().name()));

        GetPerformanceParameterResponseThrift response;

        Instance driverContainer = instanceStore
            .get(new InstanceId(driverMetadata.getDriverContainerId()));
        if (driverContainer == null) {
          Validate.isTrue(false, "can not get driver container by id" + driverMetadata);
        }
        EndPoint driverContainerEp = driverContainer.getEndPoint();

        logger.debug("try to get performance from driver container:[{}]", driverContainerEp);
        try {
          clientDt = driverContainerClientFactory.build(driverContainerEp, 2000);

          logger.warn("pullPerformanceParameter driverRequest: {}", getPerformanceRequestThrift);
          response = clientDt.pullPerformanceParameter(getPerformanceRequestThrift);
        } catch (Exception e) {
          logger.warn("Unable to pull performance from driver container:{}", driverContainerEp);
          continue;
        }

        if (response != null && (response.getReadCounter() > 0 || response.getWriteCounter() > 0)) {
          if (response.getReadLatencyNs() > 0) {
            readLatency.setFirst(readLatency.getFirst() + 1);
            readLatency.setSecond(
                readLatency.getSecond() + response.getReadLatencyNs() / response.getReadCounter());
          }

          if (response.getWriteLatencyNs() > 0) {
            writeLatency.setFirst(writeLatency.getFirst() + 1);
            writeLatency.setSecond(
                writeLatency.getSecond() + response.getWriteLatencyNs() / response
                    .getWriteCounter());
          }

          if (response.getRecordTimeIntervalMs() > 0) {
            readIops += response.getReadCounter() * 1000 / response.getRecordTimeIntervalMs();
            readThroughput += response.getReadDataSizeBytes() / response.getRecordTimeIntervalMs();

            writeIops += response.getWriteCounter() * 1000 / response.getRecordTimeIntervalMs();
            writeThroughput +=
                response.getWriteDataSizeBytes() / response.getRecordTimeIntervalMs();
          }

          logger.debug("pull performance info from driver container:[{}], response:{}",
              driverContainerEp,
              response);
        }
      }

      returnResponse.setReadIoPs(readIops);
      returnResponse.setReadThroughput(readThroughput);
      returnResponse.setWriteIoPs(writeIops);
      returnResponse.setWriteThroughput(writeThroughput);
      if (readLatency.getFirst() > 0) {
        returnResponse.setReadLatency(readLatency.getSecond() / readLatency.getFirst());
      }
      if (writeLatency.getFirst() > 0) {
        returnResponse.setWriteLatency(writeLatency.getSecond() / writeLatency.getFirst());
      }
    } catch (Exception e) {
      logger.warn("can't get performances ", e);
    }

    logger.warn("pullPerformanceParameter response:{}", returnResponse);
    return returnResponse;
  }

  /**
   * get the Iops and throughput of the selected storage pool in the past one hour.
   */
  @Override
  public GetStoragePerformanceFromPyMetricsResponseThrift pullStoragePerformanceFromPyMetrics(
      GetStoragePerformanceParameterRequestThrift request)
      throws VolumeHasNotBeenLaunchedExceptionThrift,
      ReadPerformanceParameterFromFileExceptionThrift,
      ServiceIsNotAvailableThrift, InvalidInputExceptionThrift, ServiceHavingBeenShutdownThrift,
      TException {
    GetStoragePerformanceFromPyMetricsResponseThrift response =
        new GetStoragePerformanceFromPyMetricsResponseThrift();

    //check the instance status
    checkInstanceStatus("pullStoragePerformanceFromPyMetrics");
    logger.warn("pullStoragePerformanceFromPyMetrics request: {}", request);

    long domainId = request.getDomainId();
    long storageId = request.getStorageId();
    response.setStorageId(storageId);
    response.setDomainId(domainId);
    response.setRequestId(request.getRequestId());
    List<Long> storagePoolIds = new ArrayList<>();
    storagePoolIds.add(storageId);

    //get storage pool thrift by storage pool id
    ListStoragePoolRequestThrift storagePoolListRequest = new ListStoragePoolRequestThrift();
    storagePoolListRequest.setRequestId(RequestIdBuilder.get());
    storagePoolListRequest.setDomainId(domainId);
    storagePoolListRequest.setStoragePoolIds(storagePoolIds);
    ListStoragePoolResponseThrift storagePoolsResponse = new ListStoragePoolResponseThrift();
    try {
      storagePoolsResponse = listStoragePools(storagePoolListRequest);
    } catch (Exception e) {
      logger.error("");
    }
    List<OneStoragePoolDisplayThrift> storagePoolDisplays = storagePoolsResponse
        .getStoragePoolDisplays();
    Validate.isTrue(storagePoolDisplays.size() == 1);
    StoragePoolThrift storagePoolThrift = storagePoolDisplays.get(0).getStoragePoolThrift();

    Set<Long> volumeIds = storagePoolThrift.getVolumeIds();
    if (volumeIds == null || volumeIds.isEmpty()) {
      logger.error("there is no volume in storage pool, storage pool id is : {}", storageId);
      throw new InvalidInputExceptionThrift();
    }
    Map<Long, Long> totalReadIops = new HashMap<>();
    Map<Long, Long> totalWriteIops = new HashMap<>();
    Map<Long, Long> totalReadThroughput = new HashMap<>();
    Map<Long, Long> totalWriteThroughput = new HashMap<>();
    for (Long volumeId : volumeIds) {
      GetPerformanceFromPyMetricsResponseThrift volumePerformanceResponse =
          new GetPerformanceFromPyMetricsResponseThrift();
      GetPerformanceParameterRequestThrift volumePerformanceRequest =
          new GetPerformanceParameterRequestThrift();
      volumePerformanceRequest.setRequestId(RequestIdBuilder.get());
      volumePerformanceRequest.setVolumeId(volumeId);
      try {
        volumePerformanceResponse = pullPerformanceFromPyMetrics(volumePerformanceRequest);
      } catch (VolumeHasNotBeenLaunchedExceptionThrift vhnbl) {
        logger.warn("volume has not been launch, volume id is {}", volumeId);
        logger.warn("may be other volume has pymetrics");
        continue;
      } catch (Exception e) {
        logger.error("catch an exception");
      }
      if (volumePerformanceResponse != null) {
        mergePerformanceParameterMap(totalReadIops, volumePerformanceResponse.getReadIoPs());
        mergePerformanceParameterMap(totalWriteIops, volumePerformanceResponse.getWriteIoPs());
        mergePerformanceParameterMap(totalReadThroughput,
            volumePerformanceResponse.getReadThroughput());
        mergePerformanceParameterMap(totalWriteThroughput,
            volumePerformanceResponse.getWriteThroughput());
      }
    }
    response.setReadIoPs(totalReadIops);
    response.setWriteIoPs(totalWriteIops);
    response.setReadThroughput(totalReadThroughput);
    response.setWriteThroughput(totalWriteThroughput);
    logger.warn("pullStoragePerformanceFromPyMetrics response: {}", response);
    return response;
  }

  /**
   * This function is a simple procedure which just invoke remote interface of information center
   * and then return the response got from the invoke to client.
   */
  @Override
  public ApplyIoLimitationsResponse applyIoLimitations(ApplyIoLimitationsRequest request)
      throws FailedToTellDriverAboutAccessRulesExceptionThrift, AccountNotFoundExceptionThrift,
      PermissionNotGrantExceptionThrift, ServiceIsNotAvailableThrift,
      ServiceHavingBeenShutdownThrift,
      TException {

    //check the instance status
    checkInstanceStatus("applyIoLimitations");
    logger.warn("applyIoLimitations request: {}", request);
    securityManager.hasPermission(request.getAccountId(), "applyIoLimitations");

    ApplyIoLimitationsResponse response = new ApplyIoLimitationsResponse();
    response.setRequestId(request.getRequestId());
    long toApplyRuleId = request.getRuleId();
    if (toApplyRuleId == 0) {
      logger.debug("default rule, do nothing");
    }

    logger.warn("toApplyRuleId {} {}", toApplyRuleId, ioLimitationStore.get(toApplyRuleId));
    IoLimitation ioLimitation = ioLimitationStore.get(toApplyRuleId);
    if (ioLimitation.getStatus() == IoLimitationStatus.DELETING) {
      logger.debug("The IoLimitation {} is deleting now, unable to apply this rule to drivers {}",
          ioLimitation,
          request.getDriverKeys());
      throw new IoLimitationIsDeleting();
    }

    response.setIoLimitationName(ioLimitation.getName());
    response.setAppliedDriverNames(new ArrayList<>());

    // check driver does exists or in deleting, deleted or dead status
    List<DriverKeyThrift> driverKeys = request.getDriverKeys();
    for (DriverKeyThrift driverKey : driverKeys) {
      DriverMetadata driverMetadata = driverStore
          .get(driverKey.getDriverContainerId(), driverKey.getVolumeId(),
              DriverType.valueOf(driverKey.getDriverType().name()), driverKey.getSnapshotId());
      if (driverMetadata == null) {
        throw new DriverNotFoundExceptionThrift();
      }
      if (driverMetadata.getDriverStatus() == DriverStatus.REMOVING) {
        throw new DriverIsLaunchingExceptionThrift();
      }

      if (ioLimitation.getLimitType() == IoLimitation.LimitType.Dynamic) {
        if (driverMetadata.getDynamicIoLimitationId() == 0) {
          logger.debug("applyIoLimitations {} on {}", ioLimitation, driverMetadata);
          driverMetadata.setDynamicIoLimitationId(ioLimitation.getId());
          driverStore.save(driverMetadata);
          response.addToAppliedDriverNames(driverMetadata.getDriverName());
        } else {
          logger.error("applyIoLimitations already applied a dynamic IoLimitation");
          response.addToAirDriverKeyList(buildThriftDriverMetadataFrom(driverMetadata));
        }
      } else {
        if (driverMetadata.getStaticIoLimitationId() == 0) {
          logger.debug("applyIoLimitations {} on {}", ioLimitation, driverMetadata);
          driverMetadata.setStaticIoLimitationId(ioLimitation.getId());
          driverStore.save(driverMetadata);
          response.addToAppliedDriverNames(driverMetadata.getDriverName());
        } else {
          logger.error("applyIoLimitations already applied a static IoLimitation");
          response.addToAirDriverKeyList(buildThriftDriverMetadataFrom(driverMetadata));
        }
      }

    }

    if (response.getAirDriverKeyList() != null && !response.getAirDriverKeyList().isEmpty()) {
      logger.warn("Unable to apply IoLimitations to specified driver in request {}",
          response.getAirDriverKeyList());
    }
    buildOperationAndSaveToDb(request.getAccountId(), null, OperationType.APPLY, TargetType.QOS,
        response.getIoLimitationName(),
        response.getAppliedDriverNames().stream().collect(Collectors.joining(", ")),
        OperationStatus.SUCCESS,
        0L, null, null);
    logger.warn("applyIoLimitations response: {}", response);
    return response;
  }

  /**
   * This function is a simple procedure which just invoke remote interface of information center
   * and then return the response got from the invoke to client.
   */
  @Override
  public CancelIoLimitationsResponse cancelIoLimitations(CancelIoLimitationsRequest request)
      throws AccessRuleNotAppliedThrift, AccountNotFoundExceptionThrift,
      PermissionNotGrantExceptionThrift,
      ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {

    //check the instance status
    checkInstanceStatus("cancelIoLimitations");
    logger.warn("cancelIoLimitations request: {}", request);
    securityManager.hasPermission(request.getAccountId(), "cancelIoLimitations");

    request.setCommit(true);
    CancelIoLimitationsResponse response = new CancelIoLimitationsResponse();
    response.setRequestId(request.getRequestId());
    long toCancelRuleId = request.getRuleId();
    if (toCancelRuleId == 0) {
      logger.debug("default rule, do nothing");
      return response;
    }

    IoLimitation ioLimitation = ioLimitationStore.get(toCancelRuleId);
    if (ioLimitation.getStatus() == IoLimitationStatus.DELETING) {
      logger
          .debug("The IoLimitation {} is deleting now, unable to cancel this rule from drivers {}",
              ioLimitation, request.getDriverKeys());
      throw new IoLimitationIsDeleting();
    }

    response.setIoLimitationName(ioLimitation.getName());
    response.setCanceledDriverNames(new ArrayList<>());

    List<DriverKeyThrift> driverKeys = request.getDriverKeys();
    for (DriverKeyThrift driverKey : driverKeys) {

      DriverMetadata driverMetadata = driverStore
          .get(driverKey.getDriverContainerId(), driverKey.getVolumeId(),
              DriverType.valueOf(driverKey.getDriverType().name()), driverKey.getSnapshotId());
      if (driverMetadata == null) {
        throw new DriverNotFoundExceptionThrift();
      }
      if (ioLimitation.getLimitType() == IoLimitation.LimitType.Dynamic) {
        if (driverMetadata.getDynamicIoLimitationId() == toCancelRuleId) {
          logger.debug("cancelIoLimitations {} on {}", ioLimitation, driverMetadata);
          driverMetadata.setDynamicIoLimitationId(0);
          driverStore.save(driverMetadata);
          response.addToCanceledDriverNames(driverMetadata.getDriverName());
        } else {
          logger.error("cancelIoLimitations fail, {} not applied on {}", ioLimitation,
              driverMetadata);
          response.addToAirDriverKeyList(
              RequestResponseHelper.buildThriftDriverMetadataFrom(driverMetadata));
        }
      } else {
        if (driverMetadata.getStaticIoLimitationId() == toCancelRuleId) {
          logger.debug("cancelIoLimitations {} on {}", ioLimitation, driverMetadata);
          driverMetadata.setStaticIoLimitationId(0);
          driverStore.save(driverMetadata);
          response.addToCanceledDriverNames(driverMetadata.getDriverName());
        } else {
          logger.error("cancelIoLimitations fail, {} not applied on {}", ioLimitation,
              driverMetadata);
          response.addToAirDriverKeyList(
              RequestResponseHelper.buildThriftDriverMetadataFrom(driverMetadata));
        }
      }
    }

    buildOperationAndSaveToDb(request.getAccountId(), null, OperationType.CANCEL, TargetType.QOS,
        response.getIoLimitationName(),
        response.getCanceledDriverNames().stream().collect(Collectors.joining(", ")),
        OperationStatus.SUCCESS,
        0L, null, null);
    logger.warn("cancelIoLimitations response: {}", response);

    return response;
  }

  /**
   * This function is a simple procedure which just invoke remote interface of information center
   * and then return the response got from the invoke to client.
   */
  @Override
  public DeleteIoLimitationsResponse deleteIoLimitations(DeleteIoLimitationsRequest request)
      throws ServiceHavingBeenShutdownThrift, FailedToTellDriverAboutAccessRulesExceptionThrift,
      AccountNotFoundExceptionThrift, PermissionNotGrantExceptionThrift,
      ServiceIsNotAvailableThrift,
      TException {

    //check the instance status
    checkInstanceStatus("deleteIoLimitations");
    logger.warn("deleteIoLimitations request: {}", request);
    securityManager.hasPermission(request.getAccountId(), "deleteIoLimitations");

    DeleteIoLimitationsResponse response = new DeleteIoLimitationsResponse();
    response.setRequestId(request.getRequestId());
    response.setDeletedIoLimitationNames(new ArrayList<>());
    if (request.getRuleIds() == null || request.getRuleIds().size() == 0) {
      logger.warn("No IoLimitations existing in request to delete");
      return response;
    }

    // get all drivers
    List<DriverMetadata> driverMetadataList = driverStore.list();

    for (long ruleId : request.getRuleIds()) {
      IoLimitation ioLimitation = ioLimitationStore.get(ruleId);
      if (ioLimitation == null) {
        logger.debug("No IoLimitations with id {}", ruleId);
        continue;
      }

      // unable to apply a deleting migrationSpeed
      if (ioLimitation.getStatus() == IoLimitationStatus.DELETING) {
        logger.warn("The IoLimitation {} is deleting now, unable to apply this rule to driver",
            ioLimitation);
        response.addToAirIoLimitationList(
            RequestResponseHelper.buildThriftIoLimitationFrom(ioLimitation));
        continue;
      }

      if (!request.isCommit()) {
        if (ioLimitation.getStatus() != IoLimitationStatus.DELETING) {
          ioLimitation.setStatus(IoLimitationStatus.DELETING);
          logger.warn("deleteIoLimitations, set the {} status to DELETING", ioLimitation);
          ioLimitationStore.save(ioLimitation);
        }
        continue;
      }
      // update driverStore
      for (DriverMetadata driverMetadataToBeUpdate : driverMetadataList) {
        if (ioLimitation.getLimitType() == IoLimitation.LimitType.Dynamic) {
          if (driverMetadataToBeUpdate.getDynamicIoLimitationId() == ruleId) {
            logger.warn("deleteIoLimitations Dynamic {} on {}", ioLimitation,
                driverMetadataToBeUpdate);
            driverMetadataToBeUpdate.setDynamicIoLimitationId(0);
            driverStore.save(driverMetadataToBeUpdate);
          }
        } else {
          if (driverMetadataToBeUpdate.getStaticIoLimitationId() == ruleId) {
            logger.warn("deleteIoLimitations static {} on {}", ioLimitation,
                driverMetadataToBeUpdate);
            driverMetadataToBeUpdate.setStaticIoLimitationId(0);
            driverStore.save(driverMetadataToBeUpdate);
          }
        }
      }
      // delete ioLimitation
      ioLimitationStore.delete(ruleId);
      response.addToDeletedIoLimitationNames(ioLimitation.getName());
    }

    if (response.getAirIoLimitationList() != null && !response.getAirIoLimitationList().isEmpty()) {
      logger.warn(
          "Unable to delete IoLimitations in request, due to exist air IoLimitations {} which are"
              + " operating by other action",
          response.getAirIoLimitationList());
    }
    buildOperationAndSaveToDb(request.getAccountId(), null, OperationType.DELETE, TargetType.QOS,
        response.getDeletedIoLimitationNames().stream().collect(Collectors.joining(", ")), "",
        OperationStatus.SUCCESS, 0L, null, null);
    logger.warn("deleteIoLimitations response: {}", response);
    return response;
  }

  /**
   * This function is a simple procedure which just invoke remote interface of information center
   * and then return the response got from the invoke to client.
   */
  @Override
  public ListIoLimitationsResponse listIoLimitations(ListIoLimitationsRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {

    //check the instance status
    checkInstanceStatus("listIoLimitations");
    logger.warn("listIoLimitations request: {}", request);

    ListIoLimitationsResponse listIoLimitationsResponse = new ListIoLimitationsResponse();
    listIoLimitationsResponse.setRequestId(request.getRequestId());

    List<IoLimitation> ioLimitationList = ioLimitationStore.list();
    logger.info("listIoLimitations : {}", ioLimitationList);

    if (ioLimitationList != null && ioLimitationList.size() > 0) {
      for (IoLimitation ioLimitation : ioLimitationList) {
        logger.debug("listIoLimitations limitationInfo {}-{} ", ioLimitation,
            ioLimitation.getEntries());
        listIoLimitationsResponse
            .addToIoLimitations(RequestResponseHelper.buildThriftIoLimitationFrom(ioLimitation));
      }
    } else {
      logger.info("listIoLimitations no rules ");
      listIoLimitationsResponse.setIoLimitations(new ArrayList<>());
    }

    logger.warn("listIoLimitations response: {}", listIoLimitationsResponse);
    return listIoLimitationsResponse;
  }
  /* IoLimitatio end     *************/

  /* MigrationRules begin *************/

  /**
   * This function is a simple procedure which just invoke remote interface of information center
   * and then return the response got from the invoke to client.
   */
  @Override
  public GetIoLimitationsResponse getIoLimitations(GetIoLimitationsRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {

    //check the instance status
    checkInstanceStatus("getIoLimitations");
    logger.warn("getIoLimitations request: {}", request);

    GetIoLimitationsResponse response = new GetIoLimitationsResponse();
    response.setRequestId(request.getRequestId());

    DriverKeyThrift driverKey = request.getDriverKey();
    DriverMetadata driverMetadata = driverStore
        .get(driverKey.getDriverContainerId(), driverKey.getVolumeId(),
            DriverType.valueOf(driverKey.getDriverType().name()), driverKey.getSnapshotId());
    if (driverMetadata == null) {
      logger.error("when getIoLimitations, get driver failed");
      throw new DriverNotFoundExceptionThrift();
    }

    if (driverMetadata.getDynamicIoLimitationId() != 0) {
      IoLimitation ioLimitation = ioLimitationStore.get(driverMetadata.getDynamicIoLimitationId());
      response.addToIoLimitations(RequestResponseHelper.buildThriftIoLimitationFrom(ioLimitation));
    }
    // get static IoLimitation
    if (driverMetadata.getStaticIoLimitationId() != 0) {
      IoLimitation ioLimitation = ioLimitationStore.get(driverMetadata.getStaticIoLimitationId());
      response.addToIoLimitations(RequestResponseHelper.buildThriftIoLimitationFrom(ioLimitation));
    }

    logger.warn("getIoLimitations response: {}", response);
    return response;
  }

  /**
   * This function is a simple procedure which just invoke remote interface of information center
   * and then return the response got from the invoke to client.
   */
  @Override
  public GetIoLimitationAppliedDriversResponse getIoLimitationAppliedDrivers(
      GetIoLimitationAppliedDriversRequest request)
      throws ServiceHavingBeenShutdownThrift, PermissionNotGrantExceptionThrift,
      ServiceIsNotAvailableThrift,
      TException {

    //check the instance status
    checkInstanceStatus("getIoLimitationAppliedDrivers");
    logger.warn("getIoLimitationAppliedDrivers request: {}", request);

    //securityManager.hasPermission(request.getAccountId(), "getIoLimitationAppliedDrivers");

    GetIoLimitationAppliedDriversResponse response = new GetIoLimitationAppliedDriversResponse();
    response.setRequestId(request.getRequestId());
    response.setDriverList(new ArrayList<>());

    IoLimitation ioLimitation = ioLimitationStore.get(request.getRuleId());
    if (ioLimitation == null) {
      logger.warn("No IoLimitations with id {}", request.getRuleId());
      throw new IoLimitationsNotExists();
    }

    // get all drivers
    List<DriverMetadata> driverMetadatalist = driverStore.list();
    if (driverMetadatalist == null || driverMetadatalist.isEmpty()) {
      logger.warn("driverStore is null");
      return response;
    }

    for (DriverMetadata driverMetadata : driverMetadatalist) {
      // check dynamic IOLimation
      if (driverMetadata.getDynamicIoLimitationId() == request.getRuleId()) {
        logger.debug("getIoLimitationAppliedDrivers {} on {}", driverMetadata, ioLimitation);
        response
            .addToDriverList(RequestResponseHelper.buildThriftDriverMetadataFrom(driverMetadata));
      }
      // check static IOLimation
      if (driverMetadata.getStaticIoLimitationId() == request.getRuleId()) {
        logger.debug("getIoLimitationAppliedDrivers {} on {}", driverMetadata, ioLimitation);
        response
            .addToDriverList(RequestResponseHelper.buildThriftDriverMetadataFrom(driverMetadata));
      }
    }

    logger.warn("getIoLimitationAppliedDrivers response: {}", response);
    return response;
  }

  /**
   * This function is a simple procedure which just invoke remote interface of information center
   * and then return the response got from the invoke to client.
   */
  @Override
  public ApplyMigrationRulesResponse applyMigrationRules(ApplyMigrationRulesRequest request)
      throws FailedToTellDriverAboutAccessRulesExceptionThrift, VolumeNotFoundExceptionThrift,
      VolumeBeingDeletedExceptionThrift, ApplyFailedDueToVolumeIsReadOnlyExceptionThrift,
      AccountNotFoundExceptionThrift, PermissionNotGrantExceptionThrift,
      ServiceHavingBeenShutdownThrift,
      ServiceIsNotAvailableThrift, TException {

    //check the instance status
    checkInstanceStatus("applyMigrationRules");
    logger.warn("applyMigrationSpeedRules request: {}", request);
    securityManager.hasPermission(request.getAccountId(), "applyMigrationRules");

    ApplyMigrationRulesResponse response = new ApplyMigrationRulesResponse();
    response.setRequestId(request.getRequestId());

    Long ruleId = request.getRuleId();
    MigrationRule migrationRule = new MigrationRule(migrationRuleStore.get(ruleId));
    // unable to apply a deleting migrationSpeed
    if (migrationRule.getStatus() == MigrationRuleStatus.DELETING) {
      logger.error(
          "when applyMigrationRules,The migrationSpeed {} is deleting now, unable to apply this "
              + "rule to pools",
          migrationRule);
      throw new MigrationRuleIsDeleting();
    }
    logger.info("when applyMigrationRules, get migrationRule success! ruleId:{}, migrationRule:{}",
        ruleId,
        migrationRule);

    response.setRuleName(migrationRule.getMigrationRuleName());
    response.setAppliedStoragePoolNames(new ArrayList<>());

    List<Long> toApplyPoolIdList = request.getStoragePoolIds();
    if (toApplyPoolIdList == null || toApplyPoolIdList.isEmpty()) {
      logger.warn("No pools exists to applying request, do nothing");
      return response;
    }
    // apply to pools
    for (long toApplyPoolId : toApplyPoolIdList) {

      StoragePool storagePoolToBeUpdate;
      try {
        storagePoolToBeUpdate = storagePoolStore.getStoragePool(toApplyPoolId);
        logger.warn("storagePoolStore {} {} ", toApplyPoolId, storagePoolToBeUpdate);
      } catch (Exception e) {
        logger.error("can not get storage pool, pollId:{}", toApplyPoolId, e);
        throw new TException(e);
      }

      if (storagePoolToBeUpdate == null) {
        logger.error("can not get storage pool, pollId:{}", toApplyPoolId);
        throw new StoragePoolNotExistedExceptionThrift();
      }

      if (storagePoolToBeUpdate.isDeleting()) {
        logger.error("can not get storage pool, pollId:{}", toApplyPoolId);
        throw new StoragePoolIsDeletingExceptionThrift();
      }

      if (storagePoolToBeUpdate.getMigrationRuleId() != 0) {
        logger.warn("storage pool already applied! change rule from {} to {}. storage pool:{}",
            storagePoolToBeUpdate.getMigrationRuleId(), ruleId, storagePoolToBeUpdate);
        response.addToAirStoragePoolList(
            RequestResponseHelper.buildThriftStoragePoolFrom(storagePoolToBeUpdate, migrationRule));
      }

      // update storagePool
      storagePoolToBeUpdate.setMigrationRuleId(migrationRule.getRuleId());
      logger.warn("applyMigrationSpeedRules {} on {}", migrationRule,
          storagePoolToBeUpdate.getPoolId());
      storagePoolStore.saveStoragePool(storagePoolToBeUpdate);
      response.addToAppliedStoragePoolNames(storagePoolToBeUpdate.getName());
    }

    if (response.getAirStoragePoolList() != null && !response.getAirStoragePoolList().isEmpty()) {
      logger.warn(
          "Unable to apply MigrationSpeedRules to specified driver in request, due to exist other"
              + " operation on the rules {}",
          response.getAirStoragePoolList());
    }
    buildOperationAndSaveToDb(request.getAccountId(), null, OperationType.APPLY, TargetType.QOS,
        response.getRuleName(),
        response.getAppliedStoragePoolNames().stream().collect(Collectors.joining(", ")),
        OperationStatus.SUCCESS, 0L, null, null);
    return response;
  }

  /**
   * This function is a simple procedure which just invoke remote interface of information center
   * and then return the response got from the invoke to client.
   */
  @Override
  public CancelMigrationRulesResponse cancelMigrationRules(CancelMigrationRulesRequest request)
      throws AccessRuleNotAppliedThrift, AccountNotFoundExceptionThrift,
      PermissionNotGrantExceptionThrift,
      ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {

    //check the instance status
    checkInstanceStatus("cancelMigrationRules");
    logger.warn("cancelMigrationSpeedRules request: {}", request);
    // securityManager.hasPermission(request.getAccountId(), "cancelMigrationRules");
    // securityManager.hasRightToAccess(request.getAccountId(), request.getDriverKey()
    // .getVolumeId());

    CancelMigrationRulesResponse response = new CancelMigrationRulesResponse();
    response.setRequestId(request.getRequestId());

    Long ruleId = request.getRuleId();
    if (ruleId == 0) {
      throw new AccessRuleNotAppliedThrift();
    }

    MigrationRule migrationSpeed = new MigrationRule(migrationRuleStore.get(ruleId));
    // unable to apply a deleting migrationSpeed
    if (migrationSpeed.getStatus() == MigrationRuleStatus.DELETING) {
      logger.debug("The migrationSpeed {} is deleting now, unable to apply this rule to pools",
          migrationSpeed);
      throw new MigrationRuleIsDeleting();
    }
    response.setRuleName(migrationSpeed.getMigrationRuleName());
    response.setCanceledStoragePoolNames(new ArrayList<>());

    // apply to pools
    List<Long> toApplyPoolIdList = request.getStoragePoolIds();
    if (toApplyPoolIdList == null || toApplyPoolIdList.isEmpty()) {
      logger.debug("No pools exists to applying request, do nothing");
      return response;
    }
    for (long toApplyPoolId : toApplyPoolIdList) {

      StoragePool storagePoolToBeUpdate;
      try {
        storagePoolToBeUpdate = storagePoolStore.getStoragePool(toApplyPoolId);
        logger.warn("storagePoolToBeUpdate {} ", storagePoolToBeUpdate);
      } catch (Exception e) {
        logger.warn("can not get storage pool", e);
        throw new NotEnoughGroupExceptionThrift();
      }
      if (storagePoolToBeUpdate == null) {
        throw new StoragePoolNotExistedExceptionThrift();
      }
      if (storagePoolToBeUpdate.isDeleting()) {
        throw new StoragePoolIsDeletingExceptionThrift();
      }

      // check apply or not
      if (storagePoolToBeUpdate.getMigrationRuleId() != ruleId) {
        throw new AccessRuleNotAppliedThrift();
      }

      // update storagePool to default
      storagePoolToBeUpdate.setMigrationRuleId(StoragePool.DEFAULT_MIGRATION_RULE_ID);
      logger.warn("CancelMigrationSpeedRulesResponse {} on {}", migrationSpeed,
          storagePoolToBeUpdate);
      storagePoolStore.saveStoragePool(storagePoolToBeUpdate);
      response.addToCanceledStoragePoolNames(storagePoolToBeUpdate.getName());
    }

    buildOperationAndSaveToDb(request.getAccountId(), null, OperationType.CANCEL, TargetType.QOS,
        response.getRuleName(),
        response.getCanceledStoragePoolNames().stream().collect(Collectors.joining(", ")),
        OperationStatus.SUCCESS, 0L, null, null);
    logger.warn("cancelMigrationSpeedRules response: {}", response);

    return response;
  }
  /*  MigrationRules end  *************/

  /**
   * This function is a simple procedure which just invoke remote interface of information center
   * and then return the response got from the invoke to client.
   */
  @Override
  public ListMigrationRulesResponse listMigrationRules(ListMigrationRulesRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {

    //check the instance status
    checkInstanceStatus("listMigrationRules");
    logger.warn("listMigrationSpeedRules request: {}", request);
    // securityManager.hasPermission(request.getAccountId(), "listMigrationRules");

    ListMigrationRulesResponse response = new ListMigrationRulesResponse();
    response.setRequestId(request.getRequestId());

    List<MigrationRuleInformation> migrationSpeedInfoList = migrationRuleStore.list();
    if (migrationSpeedInfoList != null && migrationSpeedInfoList.size() > 0) {
      for (MigrationRuleInformation migrationSpeedInfo : migrationSpeedInfoList) {
        response.addToMigrationRules(
            RequestResponseHelper
                .buildMigrationRuleThriftFrom(new MigrationRule(migrationSpeedInfo)));
      }
    } else {
      // set null list
      response.setMigrationRules(new ArrayList<>());
    }
    logger.warn("listMigrationSpeedRules response: {}", response);
    return response;
  }

  /**
   * This function is a simple procedure which just invoke remote interface of information center
   * and then return the response got from the invoke to client.
   */
  @Override
  public GetMigrationRulesResponse getMigrationRules(GetMigrationRulesRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {

    //check the instance status
    checkInstanceStatus("getMigrationRules");
    logger.warn("getMigrationSpeedRules request: {}", request);
    // securityManager.hasPermission(request.getAccountId(), "getMigrationRules");

    GetMigrationRulesResponse response = new GetMigrationRulesResponse();
    response.setRequestId(request.getRequestId());
    response.setAccessRules(new ArrayList<>());

    // get all storage pools
    StoragePool storagePool;
    try {
      storagePool = storagePoolStore.getStoragePool(request.getStoragePoolId());
    } catch (Exception e) {
      logger.error("can not get storage pool", e);
      throw new TException();
    }

    if (storagePool == null) {
      logger.error("failed to get storage pool");
      return response;
    }

    MigrationRule migrationSpeed = new MigrationRule(
        migrationRuleStore.get(storagePool.getMigrationRuleId()));
    MigrationRuleThrift migrationSpeedToRemote = RequestResponseHelper
        .buildMigrationRuleThriftFrom(migrationSpeed);
    response.addToAccessRules(migrationSpeedToRemote);

    logger.warn("getMigrationSpeedRules response: {}", response);
    return response;
  }

  /******    disk  info begin . **********/
  @Deprecated
  @Override
  public OnlineDiskResponse onlineDisk(OnlineDiskRequest request)
      throws DiskNotFoundExceptionThrift, DiskHasBeenOnlineThrift, ServiceHavingBeenShutdownThrift,
      AccessDeniedExceptionThrift, PermissionNotGrantExceptionThrift, ServiceIsNotAvailableThrift,
      AccountNotFoundExceptionThrift, NetworkErrorExceptionThrift, TException {

    //check the instance status
    checkInstanceStatus("onlineDisk");
    logger.warn("onlineDisk request: {}", request);
    securityManager.hasPermission(request.getAccountId(), "onlineDisk");

    EndPoint endpoint = EndPoint.fromString(request.getInstance().getEndpoint());
    try {
      DataNodeService.Iface dataNodeClient = dataNodeClientFactory
          .generateSyncClient(endpoint, timeout);
      dataNodeClient.onlineDisk(request);
    } catch (DiskNotFoundExceptionThrift e) {
      logger.error("disk not found", e);
      throw e;
    } catch (DiskHasBeenOnlineThrift e) {
      logger.error("disk has been unlaunchde", e);
      throw e;
    } catch (GenericThriftClientFactoryException e) {
      logger.error("caught a exception ", e);
      throw new NetworkErrorExceptionThrift().setDetail(e.getMessage());
    } catch (ServiceHavingBeenShutdownThrift e) {
      logger.error("caught a exception ", e);
      throw e;
    } catch (InternalErrorThrift e) {
      logger.error("Caught an exception", e);
      throw new TException(e);
    } catch (TException e) {
      logger.error("Caught an exception", e);
      throw filterTexception(e);
    } catch (Exception e) {
      logger.error("caught a exception ", e);
      throw new TException(e);
    }

    buildOperationAndSaveToDb(request.getAccountId(), null, OperationType.ONLINE, TargetType.DISK,
        request.getInstance().getEndpoint() + " " + request.getOnlineArchive().getDevName(), "",
        OperationStatus.SUCCESS, 0L, null, null);
    OnlineDiskResponse response = new OnlineDiskResponse(request.getRequestId());
    logger.warn("onlineDisk response: {}", response);
    return response;
  }

  @Override
  public SettleArchiveTypeResponse settleArchiveType(SettleArchiveTypeRequest request)
      throws DiskNotFoundExceptionThrift, DiskSizeCanNotSupportArchiveTypesThrift,
      ServiceHavingBeenShutdownThrift, ArchiveManagerNotSupportExceptionThrift,
      ServiceIsNotAvailableThrift,
      PermissionNotGrantExceptionThrift, DiskHasBeenOnlineThrift, NetworkErrorExceptionThrift,
      TException {

    //check the instance status
    checkInstanceStatus("settleArchiveType");
    logger.warn("settleArchiveType request: {}", request);
    securityManager.hasPermission(request.getAccountId(), "onlineDisk");

    EndPoint endpoint = EndPoint.fromString(request.getInstance().getEndpoint());
    try {
      DataNodeService.Iface dataNodeClient = dataNodeClientFactory
          .generateSyncClient(endpoint, timeout);
      dataNodeClient.settleArchiveType(request);
    } catch (DiskNotFoundExceptionThrift e) {
      logger.error("disk not found", e);
      throw e;
    } catch (DiskSizeCanNotSupportArchiveTypesThrift e) {
      logger.error("disk size cannot support archive types.", e);
      throw e;
    } catch (ArchiveManagerNotSupportExceptionThrift e) {
      logger.error("archive manager not support.", e);
      throw e;
    } catch (DiskHasBeenOnlineThrift e) {
      logger.error("disk has been unlaunched", e);
      throw e;
    } catch (GenericThriftClientFactoryException e) {
      logger.error("catch an exception", e);
      throw new NetworkErrorExceptionThrift().setDetail(e.getMessage());
    } catch (InternalErrorThrift e) {
      logger.error("catch an exception", e);
      throw new TException(e);
    } catch (TException e) {
      logger.error("catch an exception", e);
      throw filterTexception(e);
    } catch (Exception e) {
      throw new TException(e);
    }

    buildOperationAndSaveToDb(request.getAccountId(), null, OperationType.ONLINE, TargetType.DISK,
        request.getInstance().getEndpoint() + " " + request.getDevName(), "",
        OperationStatus.SUCCESS, 0L, null,
        null);

    SettleArchiveTypeResponse response = new SettleArchiveTypeResponse(request.getRequestId());
    logger.warn("settleArchiveType response: {}", response);
    return response;
  }

  @Override
  public OfflineDiskResponse offlineDisk(OfflineDiskRequest request)
      throws DiskNotFoundExceptionThrift, DiskHasBeenOfflineThrift, ServiceHavingBeenShutdownThrift,
      AccessDeniedExceptionThrift, DiskIsBusyThrift, PermissionNotGrantExceptionThrift,
      ServiceIsNotAvailableThrift, AccountNotFoundExceptionThrift, NetworkErrorExceptionThrift,
      TException {

    //check the instance status
    checkInstanceStatus("offlineDisk");
    logger.warn("offlineDisk request: {}", request);
    securityManager.hasPermission(request.getAccountId(), "offlineDisk");

    EndPoint endpoint = EndPoint.fromString(request.getInstance().getEndpoint());
    try {
      DataNodeService.Iface dataNodeClient = dataNodeClientFactory
          .generateSyncClient(endpoint, timeout);
      dataNodeClient.offlineDisk(request);
    } catch (DiskNotFoundExceptionThrift e) {
      logger.error("disk not found", e);
      throw e;
    } catch (DiskHasBeenOfflineThrift e) {
      logger.error("disk has been unlaunchde", e);
      throw e;
    } catch (DiskIsBusyThrift e) {
      logger.error("disk is busy", e);
      throw e;
    } catch (GenericThriftClientFactoryException e) {
      logger.error("caught a exception ", e);
      throw new NetworkErrorExceptionThrift().setDetail(e.getMessage());
    } catch (InternalErrorThrift e) {
      logger.error("caught a exception ", e);
      throw new TException(e);
    } catch (TException e) {
      logger.error("Caught an exception", e);
      throw filterTexception(e);
    } catch (Exception e) {
      logger.error("Caught an exception", e);
      throw new TException(e);
    }

    buildOperationAndSaveToDb(request.getAccountId(), null, OperationType.OFFLINE, TargetType.DISK,
        request.getInstance().getEndpoint() + " " + request.getOfflineArchive().getDevName(), "",
        OperationStatus.SUCCESS, 0L, null, null);
    OfflineDiskResponse response = new OfflineDiskResponse(request.getRequestId());
    logger.warn("offlineDisk response: {}", response);
    return response;
  }
  /*    disk  info end  **********/

  /******    list and get info begin.  **********/

  @Deprecated
  @Override
  public OnlineDiskResponse fixBrokenDisk(OnlineDiskRequest request)
      throws DiskNotFoundExceptionThrift, DiskNotBrokenThrift, ServiceHavingBeenShutdownThrift,
      AccessDeniedExceptionThrift, PermissionNotGrantExceptionThrift, ServiceIsNotAvailableThrift,
      AccountNotFoundExceptionThrift, NetworkErrorExceptionThrift, TException {

    //check the instance status
    checkInstanceStatus("fixBrokenDisk");
    logger.warn("fixBrokenDisk request: {}", request);
    securityManager.hasPermission(request.getAccountId(), "fixBrokenDisk");

    EndPoint endpoint = EndPoint.fromString(request.getInstance().getEndpoint());
    try {
      DataNodeService.Iface dataNodeClient = dataNodeClientFactory
          .generateSyncClient(endpoint, timeout);
      dataNodeClient.fixBrokenDisk(request);
    } catch (DiskNotFoundExceptionThrift e) {
      logger.error("disk not found", e);
      throw e;
    } catch (DiskNotBrokenThrift e) {
      logger.error("disk has been unlaunched", e);
      throw e;
    } catch (GenericThriftClientFactoryException e) {
      logger.error("caught a exception ", e);
      throw new NetworkErrorExceptionThrift().setDetail(e.getMessage());
    } catch (InternalErrorThrift e) {
      logger.error("caught a exception ", e);
      throw new TException(e);
    } catch (TException e) {
      logger.error("Caught an exception", e);
      throw filterTexception(e);
    } catch (Exception e) {
      logger.error("Caught an exception", e);
      throw new TException(e);
    }

    OnlineDiskResponse response = new OnlineDiskResponse(request.getRequestId());
    logger.warn("fixBrokenDisk response: {}", response);
    return response;
  }

  @Override
  public OnlineDiskResponse fixConfigMismatchDisk(OnlineDiskRequest request)
      throws DiskNotFoundExceptionThrift, DiskNotMismatchConfigThrift,
      ServiceHavingBeenShutdownThrift,
      AccessDeniedExceptionThrift, PermissionNotGrantExceptionThrift, ServiceIsNotAvailableThrift,
      AccountNotFoundExceptionThrift, NetworkErrorExceptionThrift, TException {

    //check the instance status
    checkInstanceStatus("fixConfigMismatchDisk");
    logger.warn("fixConfigMismatchDisk request: {}", request);
    securityManager.hasPermission(request.getAccountId(), "fixConfigMismatchDisk");

    EndPoint endpoint = EndPoint.fromString(request.getInstance().getEndpoint());
    try {
      DataNodeService.Iface dataNodeClient = dataNodeClientFactory
          .generateSyncClient(endpoint, timeout);
      dataNodeClient.fixConfigMismatchedDisk(request);
    } catch (DiskNotFoundExceptionThrift e) {
      logger.error("disk not found", e);
      throw e;
    } catch (DiskNotMismatchConfigThrift e) {
      logger.error("disk is not mismatched", e);
      throw e;
    } catch (GenericThriftClientFactoryException e) {
      logger.error("catch an exception", e);
      throw new NetworkErrorExceptionThrift().setDetail(e.getMessage());
    } catch (InternalErrorThrift e) {
      logger.error("catch an exception", e);
      throw new TException(e);
    } catch (TException e) {
      logger.error("Caught an exception", e);
      throw filterTexception(e);
    } catch (Exception e) {
      logger.error("Caught an exception", e);
      throw new TException(e);
    }

    buildOperationAndSaveToDb(request.getAccountId(), null, OperationType.ONLINE, TargetType.DISK,
        request.getInstance().getEndpoint() + " " + request.getOnlineArchive().getDevName(), "",
        OperationStatus.SUCCESS, 0L, null, null);

    OnlineDiskResponse response = new OnlineDiskResponse(request.getRequestId());
    logger.warn("fixConfigMismatchDisk response: {}", response);
    return response;
  }

  /**
   * An implementation of interface.
   * {@link InformationCenter.Iface#getAppliedStoragePools(GetAppliedStoragePoolsRequest)}
   * list pools on which this migrateSpeed applied.
   */

  @Override
  public GetAppliedStoragePoolsResponse getAppliedStoragePools(
      GetAppliedStoragePoolsRequest request)
      throws ServiceHavingBeenShutdownThrift, PermissionNotGrantExceptionThrift,
      MigrationRuleNotExists,
      TException {
    //check the instance status
    checkInstanceStatus("getAppliedStoragePools");
    logger.warn("getAppliedStoragePools request: {}", request);

    MigrationRuleInformation migrationRuleInformation = migrationRuleStore.get(request.getRuleId());
    if (migrationRuleInformation == null) {
      logger.error("can not found migration rule by:{}", request.getRuleId());
      throw new MigrationRuleNotExists();
    }

    MigrationRule migrationRule = migrationRuleInformation.toMigrationRule();
    GetAppliedStoragePoolsResponse response = new GetAppliedStoragePoolsResponse();
    response.setRequestId(request.getRequestId());
    response.setStoragePoolList(new ArrayList<>());

    // get all storage pools
    List<StoragePool> storagePools = null;
    try {
      storagePools = storagePoolStore.listAllStoragePools();
    } catch (Exception e) {
      logger.error("can not get storage pool", e);
    }

    if (storagePools == null) {
      logger.error("failed to get storage pool");
      return response;
    }

    for (StoragePool storagePoolToBeUpdate : storagePools) {
      logger.debug("getAppliedStoragePools MigrationSpeedId {} ruleId {}",
          storagePoolToBeUpdate.getMigrationRuleId(), request.getRuleId());

      if (storagePoolToBeUpdate.getMigrationRuleId() != StoragePool.DEFAULT_MIGRATION_RULE_ID
          .longValue()
          && storagePoolToBeUpdate.getMigrationRuleId() == request.getRuleId()) {
        // update storagePool to default
        logger.warn("getAppliedStoragePools on {}", request.getRuleId());
        response.addToStoragePoolList(
            RequestResponseHelper.buildThriftStoragePoolFrom(storagePoolToBeUpdate, migrationRule));
      }
    }

    logger.warn("getAppliedStoragePools response: {}", response);
    return response;
  }

  @Override
  public ListStoragePoolCapacityResponseThrift listStoragePoolCapacity(
      ListStoragePoolCapacityRequestThrift request)
      throws ServiceHavingBeenShutdownThrift, InvalidInputExceptionThrift,
      ServiceIsNotAvailableThrift,
      TException {
    //check the instance status
    checkInstanceStatus("listStoragePoolCapacity");
    logger.warn("listStoragePoolCapacity request: {}", request);

    final ListStoragePoolCapacityResponseThrift response =
        new ListStoragePoolCapacityResponseThrift();

    try {
      ValidateParam.validateListStoragePoolCapacityRequest(request);
    } catch (InvalidInputExceptionThrift e) {
      logger.error("invalid input");
      throw e;
    }

    // check if list some or all storage pool
    List<StoragePool> storagePoolList = new ArrayList<>();
    if (request.isSetStoragePoolIdList()) {
      logger.debug("list some storage pool");
      try {
        storagePoolList.addAll(storagePoolStore.listStoragePools(request.getStoragePoolIdList()));
      } catch (Exception e) {
        logger.warn("can not list storage pool", e);
      }
    } else {
      logger.debug("list all storage pool");
      try {
        storagePoolList.addAll(storagePoolStore.listStoragePools(request.getDomainId()));
      } catch (Exception e) {
        logger.warn("can not list storage pool", e);
      }
    }
    if (storagePoolList == null || storagePoolList.isEmpty()) {
      logger.error("storage pool is empty");
      throw new StorageEmptyExceptionThrift();
    }

    logger.debug("storage pool list is {}", storagePoolList);
    List<StoragePoolCapacityThrift> storagePoolCapacityThriftList =
        new ArrayList<StoragePoolCapacityThrift>();
    for (StoragePool storagePool : storagePoolList) {
      long volumeFreeSpace = 0L; //the space allocate for volume but not use
      long volumeSpace = 0L; ////the space allocate for volume
      for (Long volumeId : storagePool.getVolumeIds()) {
        VolumeMetadata volumeMetadata = null;
        try {
          volumeMetadata = volumeInformationManger
              .getVolumeNew(volumeId.longValue(), request.getAccountId());
        } catch (VolumeNotFoundException e) {
          logger.warn("when listStoragePoolCapacity can not find the volume: {}", volumeId);
          continue;
        }

        if (volumeMetadata.getVolumeStatus() == VolumeStatus.Dead) {
          logger.warn("volume status is Dead, volumeId is: {}", volumeId);
          continue;
        }
        int writeCount =
            volumeMetadata.getVolumeType().getNumSecondaries() + 1; //secondary + primary
        for (SegmentMetadata segmentMetadata : volumeMetadata.getSegments()) {
          volumeFreeSpace += segmentSize * writeCount * segmentMetadata.getFreeRatio();
          volumeSpace += segmentSize * writeCount;

          logger.debug("the segment:{} free ratio is {}. VolumeType:{}", segmentMetadata.getSegId(),
              segmentMetadata.getFreeRatio(), volumeMetadata.getVolumeType());
        }
        logger
            .debug("volume:{} has segment size:{}", volumeId, volumeMetadata.getSegments().size());
      }
      logger.info("the allocate space is {} ,not used space is {},", volumeSpace, volumeFreeSpace);
      StoragePoolCapacityThrift storagePoolCapacityThrift = new StoragePoolCapacityThrift();
      storagePoolCapacityThrift.setDomainId(storagePool.getDomainId());
      storagePoolCapacityThrift.setStoragePoolId(storagePool.getPoolId());
      storagePoolCapacityThrift.setStoragePoolName(storagePool.getName());
      //storagePoolCapacityThrift.setUsedSpace(volumeSpace - volumeFreeSpace);
      long freeSpace = 0L;
      long totalSpace = 0L;
      long usedSpace = 0L;
      for (Entry<Long, Long> entry : storagePool.getArchivesInDataNode().entries()) {
        Long datanodeId = entry.getKey();
        Long archiveId = entry.getValue();
        logger.debug("datanode id is: {} and archive id is: {}", datanodeId, archiveId);
        InstanceMetadata datanode = storageStore.get(datanodeId);
        if (datanode != null && datanode.getDatanodeStatus().equals(OK)) {
          RawArchiveMetadata archive = datanode.getArchiveById(archiveId);
          if (archive != null) {
            freeSpace += archive.getLogicalFreeSpace();
            totalSpace += archive.getLogicalSpace();
            usedSpace += archive.getUsedSpace();

          }
        }
      }
      logger.warn("free space is {}, total space is {}, usedd is {}", freeSpace, totalSpace,
          usedSpace);
      storagePoolCapacityThrift.setUsedSpace(usedSpace);
      storagePoolCapacityThrift.setFreeSpace(freeSpace);
      storagePoolCapacityThrift.setTotalSpace(totalSpace);
      storagePoolCapacityThriftList.add(storagePoolCapacityThrift);
      logger.debug("storage pool capacity thrift is {}", storagePoolCapacityThrift);
    }
    logger.debug("storage pool capacity thrift list size is {}",
        storagePoolCapacityThriftList.size());
    response.setStoragePoolCapacityList(storagePoolCapacityThriftList);
    response.setRequestId(request.getRequestId());
    logger.warn("listStoragePoolCapacity response: {}", response);
    return response;
  }

  @Override
  public GetDriversResponseThrift getDrivers(GetDriversRequestThrift request)
      throws VolumeNotFoundExceptionThrift, ServiceHavingBeenShutdownThrift,
      ServiceIsNotAvailableThrift,
      TException {

    //check the instance status
    checkInstanceStatus("getDrivers");
    logger.warn("getDrivers request: {}", request);

    GetDriversResponseThrift returnResponse = new GetDriversResponseThrift();
    returnResponse.setRequestId(request.getRequestId());

    VolumeMetadata volumeMetadata = volumeStore.getVolume(request.getVolumeId());

    if (volumeMetadata == null) {
      logger.error("No volume found with id {}", request.getVolumeId());
      throw new VolumeNotFoundExceptionThrift();
    }

    // get all driver bound to the volume
    List<DriverMetadata> driversList = driverStore.get(request.getVolumeId());
    if ((driversList == null) || (driversList.isEmpty())) {
      logger.debug("No drivers launched");
      return returnResponse;
    } else {
      List<DriverMetadataThrift> driversThriftList = new ArrayList<>();
      for (DriverMetadata driverMetadata : driversList) {
        driversThriftList.add(RequestResponseHelper.buildThriftDriverMetadataFrom(driverMetadata));
      }
      returnResponse.setDrivers(driversThriftList);
    }

    logger.warn("getDrivers response: {}", returnResponse);
    return returnResponse;
  }

  /**
   * list archives of all datanode instances.
   ****/
  @Override
  public ListArchivesResponseThrift listArchives(ListArchivesRequestThrift request)
      throws InternalErrorThrift, ServiceIsNotAvailableThrift, TException {

    //check the instance status
    checkInstanceStatus("listArchives");

    logger.info("listArchives request: {}", request);

    //get instance disk info
    Map<String, Map<String, DiskInfo>> instanceIp2DiskInfoMap = getInstanceDiskInfo();

    ListArchivesResponseThrift response = new ListArchivesResponseThrift();
    List<InstanceMetadataThrift> instanceMetadataThriftList = new ArrayList<>();
    for (InstanceMetadata datanode : storageStore.list()) {
      EndPoint endPoint = EndPoint.fromString(datanode.getEndpoint());
      boolean networkSubhealthy = determinDataNodeIsNetworkSubhealthy(endPoint.getHostName());
      InstanceMetadataThrift instanceMetadataThrift;
      if (networkSubhealthy) {
        instanceMetadataThrift = RequestResponseHelper
            .buildThriftInstanceFromSeparatedDatanode(datanode);
      } else {
        instanceMetadataThrift = RequestResponseHelper.buildThriftInstanceFrom(datanode);
      }

      // set archive rate
      if (instanceIp2DiskInfoMap.containsKey(instanceMetadataThrift.getEndpoint())) {
        Map<String, DiskInfo> diskName2DiskInfoMap = instanceIp2DiskInfoMap
            .get(instanceMetadataThrift.getEndpoint());
        for (ArchiveMetadataThrift archiveMetadataThrift : instanceMetadataThrift
            .getArchiveMetadata()) {
          if (diskName2DiskInfoMap.containsKey(archiveMetadataThrift.getDevName())) {
            archiveMetadataThrift
                .setRate(diskName2DiskInfoMap.get(archiveMetadataThrift.getDevName()).getRate());
          }
        }
      }

      instanceMetadataThriftList.add(instanceMetadataThrift);
    }

    response.setRequestId(request.getRequestId());
    response.setInstanceMetadata(instanceMetadataThriftList);

    logger.warn("listArchives response: {}", response);
    return response;
  }

  /**
   * filter archives
   *
   * <p>if archive is mix archive, and this mix archive make up with raw, so this disk in
   * rawArchives and archiveMetadata of instance, then we must remove it from rawArchives. if we not
   * filter, we will see two archives on console
   */
  @Override
  public ListArchivesAfterFilterResponseThrift ListArchivesAfterFilter(
      ListArchivesAfterFilterRequestThrift request)
      throws TException {
    if (shutDownFlag) {
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (InstanceStatus.SUSPEND == appContext.getStatus()) {
      logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
      throw new ServiceIsNotAvailableThrift().setDetail("I am suspend");
    }

    logger.warn("ListArchivesAfterFilter request: {}", request);

    ListArchivesAfterFilterResponseThrift response = new ListArchivesAfterFilterResponseThrift();
    List<InstanceMetadataThrift> archivesThrift = new ArrayList<>();
    for (InstanceMetadata instance : storageStore.list()) {
      if (instance.getDatanodeStatus().equals(OK)) {
        archivesThrift.add(RequestResponseHelper.buildThriftInstanceFromAfterFilter(instance));
      }
    }

    response.setRequestId(request.getRequestId());
    response.setInstanceMetadata(archivesThrift);

    logger.warn("ListArchivesAfterFilter response: {}", response);
    return response;
  }

  @Override
  public GetArchiveResponseThrift getArchive(GetArchiveRequestThrift request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {

    //check the instance status
    checkInstanceStatus("getArchive");
    logger.warn("getArchive request: {}", request);

    GetArchiveResponseThrift response = new GetArchiveResponseThrift();
    List<Long> archiveIds = request.getArchiveIds();
    List<InstanceMetadataThrift> archivesThrift = new ArrayList<>();

    for (Long archiveId : archiveIds) {
      OUT:
      for (InstanceMetadata instance : storageStore.list()) {
        if (instance.getDatanodeStatus().equals(OK)) {
          for (RawArchiveMetadata rawArchiveMetadata : instance.getArchives()) {
            if (rawArchiveMetadata != null && rawArchiveMetadata.getArchiveId().equals(archiveId)) {
              archivesThrift.add(RequestResponseHelper
                  .buildThriftInstanceFromForArchiveId(instance, archiveId));
              break OUT;
            }
          }

          for (ArchiveMetadata archiveMetadata : instance.getArchiveMetadatas()) {
            if (archiveMetadata != null && archiveMetadata.getArchiveId().equals(archiveId)) {
              archivesThrift.add(RequestResponseHelper
                  .buildThriftInstanceFromForArchiveId(instance, archiveId));
              break OUT;
            }
          }

        }
      }
    }

    response.setInstanceMetadata(archivesThrift);
    response.setRequestId(request.getRequestId());
    logger.warn("getArchive response: {}", response);
    return response;
  }

  /**
   * list archive of the specified datanode instance.
   */
  @Override
  public GetArchivesResponseThrift getArchives(GetArchivesRequestThrift request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {
    //check the instance status
    checkInstanceStatus("getArchives");
    logger.warn("getArchives request: {}", request);

    GetArchivesResponseThrift response = new GetArchivesResponseThrift();
    InstanceMetadata instance = storageStore.get(request.getInstanceId());
    if (null != instance) {
      response.setInstanceMetadata(RequestResponseHelper.buildThriftInstanceFrom(instance));
    }

    response.setRequestId(request.getRequestId());
    logger.warn("getArchives response: {}", response);
    return response;
  }

  @Override
  public OrphanVolumeResponse listOrphanVolume(OrphanVolumeRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {
    //check the instance status
    checkInstanceStatus("listOrphanVolume");
    logger.warn("listOrphanVolume request: {}", request);

    List<Long> orphanVolumeIds = orphanVolumes.getOrphanVolume();
    OrphanVolumeResponse response = new OrphanVolumeResponse();
    List<VolumeMetadataThrift> orphanVolumeThriftList = new ArrayList<VolumeMetadataThrift>();

    for (Long orphanVolumeId : orphanVolumeIds) {
      VolumeMetadata orphanRootVolume;
      try {
        orphanRootVolume = volumeInformationManger
            .getVolumeNew(orphanVolumeId, request.getAccountId());
      } catch (VolumeNotFoundException e) {
        logger.error("when listOrphanVolume, can not find the volume :{}", orphanVolumeId);
        continue;
      }

      // if volume status is dead, no need to display at console
      if (orphanRootVolume.getVolumeStatus().equals(VolumeStatus.Dead)) {
        logger.debug("volume is in dead status, volume id is {}, name is {}",
            orphanRootVolume.getVolumeId(),
            orphanRootVolume.getName());
        continue;
      }
      orphanVolumeThriftList
          .add(RequestResponseHelper.buildThriftVolumeFrom(orphanRootVolume, false));
    }

    response.setRequestId(request.getRequestId());
    response.setOrphanVolumes(orphanVolumeThriftList);
    logger.warn("listOrphanVolume response: {}", response);
    return response;
  }

  @Override
  public ListVolumesResponse listVolumes(ListVolumesRequest request)
      throws AccessDeniedExceptionThrift, ServiceHavingBeenShutdownThrift,
      ServiceIsNotAvailableThrift,
      AccountNotFoundExceptionThrift, ResourceNotExistsExceptionThrift, TException {

    //check the instance status
    checkInstanceStatus("listVolumes");
    logger.warn("listVolumes request: {}", request);

    Long accountId = request.getAccountId();
    final Set<Long> accessibleResource = securityManager
        .getAccessibleResourcesByType(accountId, PyResource.ResourceType.Volume);
    Set<Long> newVolumesCanBeList;
    if (request.isSetVolumesCanBeList()) {
      newVolumesCanBeList = request.getVolumesCanBeList().stream()
          .filter(l -> accessibleResource.contains(l))
          .collect(Collectors.toSet());
    } else {
      newVolumesCanBeList = accessibleResource;
    }

    request.setVolumesCanBeList(newVolumesCanBeList);
    logger.warn("listVolumes, get the newVolumesCanBeList: {}", newVolumesCanBeList);

    ListVolumesResponse response = new ListVolumesResponse();
    try {
      List<VolumeMetadataThrift> volumesThrift = new ArrayList<VolumeMetadataThrift>();

      Set<Long> volumesCanBeList = null;
      if (request.isSetVolumesCanBeList()) {
        volumesCanBeList = request.getVolumesCanBeList();
      }

      List<VolumeMetadata> allVolumes = volumeStore.listVolumes();
      logger.info("the listVolumes, id :{}, status: {}",
          allVolumes.stream().map(VolumeMetadata::getVolumeId)
              .collect(Collectors.toList()),
          allVolumes.stream().map(VolumeMetadata::getVolumeStatus)
              .collect(Collectors.toList()));
      logger.debug("All volumes are {}", allVolumes);
      boolean containDeadVolume = false;
      if (request.isSetContainDeadVolume()) {
        containDeadVolume = request.isContainDeadVolume();
      }

      for (VolumeMetadata volumeMetadata : allVolumes) {
        if (volumesCanBeList != null && !volumesCanBeList.contains(volumeMetadata.getVolumeId())) {
          continue;
        }

        logger.warn("Current volume:{}, and markDelete:{}", volumeMetadata.getName(),
            volumeMetadata.isMarkDelete());
        /* if volume status is dead, no need to display at console **/
        if (!containDeadVolume) {
          if (volumeMetadata.getVolumeStatus().equals(VolumeStatus.Dead)) {
            logger.debug("volume is in dead status, volume id is {}, name is {}",
                volumeMetadata.getVolumeId(), volumeMetadata.getName());
            continue;
          }
        }
        if (volumeMetadata.isMarkDelete()) {
          logger.warn("volume:{} {} has been marked delete, so do not display any more",
              volumeMetadata.getVolumeId(), volumeMetadata.getName());
          continue;
        }

        //make thre RebalanceInfo
        volumeMetadata.getRebalanceInfo().calcRatio();
        VolumeMetadataThrift volumeMetadataThrift = RequestResponseHelper
            .buildThriftVolumeFrom(volumeMetadata, false);
        volumesThrift.add(volumeMetadataThrift);
      }

      response.setRequestId(request.getRequestId());
      response.setVolumes(volumesThrift);

      // avoid list volume got nothing, but volumes still exist
      if (response.getVolumes() != null && !response.getVolumes().isEmpty()) {
        // this is for delete the resource after volume is deleted and removed from volumeJobStore
        for (Long volumeId : newVolumesCanBeList) {
          boolean found = false;
          for (VolumeMetadataThrift volumeMetadataThrift : response.getVolumes()) {
            if (volumeMetadataThrift.getVolumeId() == volumeId) {
              found = true;
              logger.warn("listVolumes, find the volume: {} not in accessibleResource", volumeId,
                  newVolumesCanBeList);
              break;
            }
          }
        }
      }
    } catch (Exception e) {
      logger.error("Caught an exception", e);
      throw new TException(e);
    }

    //for volume DeleteDelay
    List<py.thrift.share.VolumeMetadataThrift> volumeMetadataThriftList = response.getVolumes();
    List<VolumeDeleteDelayInformation> volumeDeleteDelayInformationList = volumeDelayStore
        .listVolumesDelayInfo();
    Map<Long, VolumeDeleteDelayInformation> volumeDeleteDelayInformationMap = new HashMap<>();
    for (VolumeDeleteDelayInformation volumeDeleteDelayInformation :
        volumeDeleteDelayInformationList) {
      volumeDeleteDelayInformationMap
          .put(volumeDeleteDelayInformation.getVolumeId(), volumeDeleteDelayInformation);
    }

    //for volume recycle
    Set<Long> recycleVolumes = new HashSet<>();
    List<VolumeRecycleInformation> volumeRecycleInformationList = volumeRecycleStore
        .listVolumesRecycleInfo();
    for (VolumeRecycleInformation volumeRecycleInformation : volumeRecycleInformationList) {
      recycleVolumes.add(volumeRecycleInformation.getVolumeId());
    }

    Iterator<VolumeMetadataThrift> iterator = volumeMetadataThriftList.iterator();
    while (iterator.hasNext()) {
      VolumeMetadataThrift volumeMetadataThrift = iterator.next();
      if (volumeDeleteDelayInformationMap.containsKey(volumeMetadataThrift.getVolumeId())) {
        response.putToVolumeDeleteDelayInfo(volumeMetadataThrift.getVolumeId(),
            RequestResponseHelper.buildThriftVolumeDeleteDelayFrom(
                volumeDeleteDelayInformationMap.get(volumeMetadataThrift.getVolumeId())));
      }

      //remove in recycle
      if (recycleVolumes.contains(volumeMetadataThrift.getVolumeId())) {
        logger
            .warn("find the volume :{} in recycle, not list", volumeMetadataThrift.getVolumeId());
        iterator.remove();
      }
    }

    logger.info("listVolumes response: {}", response);
    return response;
  }

  /**
   * The method use to get different drivers by different request from driverStore.
   */
  @Override
  public ListAllDriversResponse listAllDrivers(ListAllDriversRequest request)
      throws ServiceIsNotAvailableThrift, ServiceHavingBeenShutdownThrift,
      ParametersIsErrorExceptionThrift {

    //check the instance status
    checkInstanceStatus("listAllDrivers");

    logger.warn("listAllDrivers request: {}", request);
    ListAllDriversResponse response = new ListAllDriversResponse();
    response.setRequestId(request.getRequestId());

    List<DriverMetadata> driverMetadatas = driverStore.list();

    if (request.isSetVolumeId()) {
      driverMetadatas = getDriversByVolumeId(driverMetadatas, request.getVolumeId());
    }
    if (request.isSetSnapshotId()) {
      driverMetadatas = getDriversBySnapshotId(driverMetadatas, request.getSnapshotId());
    }

    if (request.isSetDrivercontainerHost()) {
      driverMetadatas = getDriversByDcHost(driverMetadatas, request.getDrivercontainerHost());
    }

    if (request.isSetDriverHost()) {
      driverMetadatas = getDriversByDriverHost(driverMetadatas, request.getDriverHost());
    }
    if (request.isSetDriverType()) {
      driverMetadatas = getDriversByDriverType(driverMetadatas,
          DriverType.valueOf(request.getDriverType().name()));
    }

    List<DriverMetadataThrift> driverMetadataThrifts = new ArrayList<>();
    for (DriverMetadata driverMetadata : driverMetadatas) {
      //check the volume name
      if (driverMetadata.getVolumeName() == null && driverMetadata.getVolumeId() != 0) {
        VolumeMetadata volumeMetadata = volumeStore.getVolume(driverMetadata.getVolumeId());
        if (volumeMetadata != null) {
          driverMetadata.setVolumeName(volumeMetadata.getName());
        }
        logger.warn(
            "when listAllDrivers, find the volume :{}, the volume name not set in DriverMetadata,"
                + "set is :{}",
            driverMetadata.getVolumeId(), driverMetadata.getVolumeName());
      }

      driverMetadataThrifts
          .add(RequestResponseHelper.buildThriftDriverMetadataFrom(driverMetadata));
    }
    response.setDriverMetadatasthrift(driverMetadataThrifts);

    logger.warn("listAllDrivers response :{}", response);
    return response;
  }

  @Override
  public ListDriverClientInfoResponse listDriverClientInfo(ListDriverClientInfoRequest request)
      throws ServiceIsNotAvailableThrift, ServiceHavingBeenShutdownThrift,
      ParametersIsErrorExceptionThrift, TException {
    checkInstanceStatus("listDriverClientInfo");

    logger.warn("listDriverClientInfo request: {}", request);

    ListDriverClientInfoResponse response = new ListDriverClientInfoResponse();
    response.setRequestId(request.getRequestId());

    List<DriverClientInfoThrift> driverClinetInfoThrifts = new LinkedList<>();
    List<DriverClientInformation> driverClientInformationGet = new LinkedList();
    List<DriverClientInformation> driverClientInformationList = driverClientManger
        .listAllDriverClientInfo();

    if (request.isSetDriverName() || request.isSetVolumeName() || request
        .isSetVolumeDescription()) {
      driverClientInformationGet = getDriverClientInfo(
          driverClientInformationList, request.getDriverName(),
          request.getVolumeName(), request.getVolumeDescription());
    } else {
      //list all
      if (driverClientInformationList != null && !driverClientInformationList.isEmpty()) {
        driverClientInformationGet = driverClientInformationList;
      }
    }

    for (DriverClientInformation driverClientInformation : driverClientInformationGet) {
      driverClinetInfoThrifts
          .add(RequestResponseHelper.buildThriftDriverMetadataFrom(driverClientInformation));
    }

    response.setDriverClientInfothrift(driverClinetInfoThrifts);

    logger.info("listDriverClientInfo response :{}", response);
    return response;
  }

  @Override
  public GetCapacityResponse getCapacity(GetCapacityRequest request)
      throws StorageEmptyExceptionThrift, ServiceHavingBeenShutdownThrift,
      ServiceIsNotAvailableThrift,
      TException {

    //check the instance status
    checkInstanceStatus("getCapacity");

    logger.warn("getCapacity request: {}", request);

   

    if (storageStore.size() == 0) {
      throw new StorageEmptyExceptionThrift();
    }

    long physicalSpaces = 0L;
    long freeSpace = 0L;
    long logicalSpaces = 0L;
    for (InstanceMetadata entry : storageStore.list()) {
      if (entry.getDatanodeStatus().equals(OK)) {
        physicalSpaces += entry.getCapacity();
        freeSpace += entry.getFreeSpace();
        logicalSpaces += entry.getLogicalCapacity();
      }
    }

    GetCapacityResponse response = new GetCapacityResponse();
    response.setRequestId(request.getRequestId());
    response.setLogicalCapacity(logicalSpaces);
    response.setFreeSpace(freeSpace);
    response.setCapacity(physicalSpaces);

    logger.warn("getCapacity response: {}", response);
    return response;
  }

  /******    list and get info end . **********/

  @Override
  public GetCapacityRecordResponseThrift getCapacityRecord(GetCapacityRecordRequestThrift request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {
    //check the instance status
    checkInstanceStatus("getCapacityRecord");
    logger.warn("getCapacityRecord request: {}", request);

    GetCapacityRecordResponseThrift response = new GetCapacityRecordResponseThrift();
    response.setRequestId(request.getRequestId());
    CapacityRecord capacityRecord;
    try {
      capacityRecord = capacityRecordStore.getCapacityRecord();
    } catch (Exception e) {
      logger.error("caught an exception");
      throw new TException();
    }
    response.setCapacityRecord(RequestResponseHelper.buildThriftCapacityRecordFrom(capacityRecord));
    logger.warn("getCapacityRecord response: {}", response);
    return response;
  }

 
 
 
 
 
  @Override
  public ListOperationsResponse listOperations(ListOperationsRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      OperationNotFoundExceptionThrift,
      PermissionNotGrantExceptionThrift, TException {
    logger.info("listOperations request: {}", request);

    ListOperationsResponse response = new ListOperationsResponse();
    response.setRequestId(request.getRequestId());

    List<Operation> operationList = operationStore.getAllOperation();
    if (request.isSetOperationIds()) {
      operationList = operationList.stream()
          .filter(o -> request.getOperationIds().contains(o.getOperationId()))
          .collect(Collectors.toList());
    }
    if (request.isSetAccountName()) {
      operationList = operationList.stream()
          .filter(o -> o.getAccountName().contains(request.getAccountName()))
          .collect(Collectors.toList());
    }
    if (request.isSetOperationType()) {
      operationList = operationList.stream()
          .filter(o -> o.getOperationType().name().equals(request.getOperationType()))
          .collect(Collectors.toList());
    }
    if (request.isSetTargetType()) {
      operationList = operationList.stream()
          .filter(o -> o.getTargetType().name().equals(request.getTargetType()))
          .collect(Collectors.toList());
    }
    if (request.isSetTargetName()) {
      operationList = operationList.stream()
          .filter(
              o -> (o.getOperationObject().contains(request.getTargetName()) || o.getTargetName()
                  .contains(request.getTargetName()))).collect(Collectors.toList());
    }
    if (request.isSetStatus()) {
      operationList = operationList.stream()
          .filter(o -> (o.getStatus().name().equals(request.getStatus())))
          .collect(Collectors.toList());
    }
    if (request.isSetStartTime()) {
      operationList = operationList.stream()
          .filter(o -> o.getStartTime() >= (request.getStartTime()))
          .collect(Collectors.toList());
    }
    if (request.isSetEndTime()) {
      operationList = operationList.stream().filter(o -> o.getEndTime() <= (request.getEndTime()))
          .collect(Collectors.toList());
    }
    boolean isAdmin = false;
    AccountMetadata accountMetadata = securityManager.getAccountById(request.getAccountId());
    for (Role role : accountMetadata.getRoles()) {
      if (role.getName().equals("admin") || role.getName().equals("superadmin")) {
        isAdmin = true;
        break;
      }
    }

    List<OperationThrift> operationListThrift = new ArrayList<>();
    for (Operation operation : operationList) {
      if (isAdmin || operation.getAccountId().longValue() == accountMetadata.getAccountId()
          .longValue()) {
        operationListThrift.add(RequestResponseHelper.buildThriftOperationFrom(operation));
      }

    }

    response.setOperationList(operationListThrift);
    logger.info("listOperations response: {}", response);
    return response;
  }

  /**** other  being.  *****/

  @Override
  public PingPeriodicallyResponse pingPeriodically(PingPeriodicallyRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {
    logger.debug("ping infocenter from daemoncommon periodlly.");

    long reportTime = System.currentTimeMillis();
    serverNodeReportTimeMap.put(request.getServerId(), reportTime);

    return new PingPeriodicallyResponse(request.getRequestId());
  }

  @Override
  public GetDashboardInfoResponse getDashboardInfo(GetDashboardInfoRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      PermissionNotGrantExceptionThrift,
      AccountNotFoundExceptionThrift, TException {

    //check the instance status
    checkInstanceStatus("getDashboardInfo");
    logger.info("getDashboardInfo request: {}", request);

    GetDashboardInfoResponse response = new GetDashboardInfoResponse();
    Capacity capacity = getSystemCapacity(request.getAccountId());
    response.setCapacity(capacity);

    InstanceStatusStatistics instanceStatusStatistics = getInstanceStatusStatistics();
    response.setInstanceStatusStatistics(instanceStatusStatistics);

    ClientTotal clientTotal = getTotalClient(request.getAccountId());
    response.setClientTotal(clientTotal);

    DiskStatistics diskStatistics = getDiskStatistics();
    response.setDiskStatistics(diskStatistics);

    ServerNodeStatistics serverNodeStatistics = getServerNodeStatistics(request.getAccountId());
    response.setServerNodeStatistics(serverNodeStatistics);

    // set volume statistics from controlcenter.
    VolumeCounts volumeCounts = getVolumeStatistics(request.getAccountId());
    response.setVolumeCounts(volumeCounts);

    // set storage statistics from controlcenter.
    PoolStatistics poolStatistics = getStorageStatistics(request);
    response.setPoolStatistics(poolStatistics);

    logger.info("getDashboardInfo response: {}", response);
    return response;
  }

  @Override
  public CreateDefaultDomainAndStoragePoolResponseThrift createDefaultDomainAndStoragePool(
      CreateDefaultDomainAndStoragePoolRequestThrift request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      DatanodeNotFoundExceptionThrift,
      DomainNameExistedExceptionThrift, DatanodeIsUsingExceptionThrift,
      DatanodeNotFreeToUseExceptionThrift,
      PermissionNotGrantExceptionThrift, InvalidInputExceptionThrift,
      AccountNotFoundExceptionThrift,
      DomainExistedExceptionThrift, DomainNotExistedExceptionThrift,
      StoragePoolNameExistedExceptionThrift,
      ArchiveIsUsingExceptionThrift, ArchiveNotFoundExceptionThrift,
      ArchiveNotFreeToUseExceptionThrift,
      StoragePoolExistedExceptionThrift, AccessDeniedExceptionThrift,
      DomainIsDeletingExceptionThrift,
      TException {

    //check the instance status
    checkInstanceStatus("createDefaultDomainAndStoragePool");
    logger.warn("createDefaultDomainAndStoragePool request: {}", request);

    CreateDefaultDomainAndStoragePoolResponseThrift response =
        new CreateDefaultDomainAndStoragePoolResponseThrift();
    response.setRequestId(request.getRequestId());
    try {
      ListArchivesRequestThrift listArchivesRequest = new ListArchivesRequestThrift();
      final ListArchivesResponseThrift listArchivesResponse = listArchives(listArchivesRequest);

      // create default domain and default storage pool
      long defaultDomainId = RequestIdBuilder.get();
      final long defaultPoolId = RequestIdBuilder.get();
      String defaultDomainName = "DefaultDomain";
      String defaultDomainDescription = "default domain for use";
      Set<Long> datanodeIds = new HashSet<Long>();
      Set<Long> storagePools = new HashSet<Long>();
      Domain defaultDomain = new Domain();
      defaultDomain.setDomainId(defaultDomainId);
      defaultDomain.setDomainName(defaultDomainName);
      defaultDomain.setDomainDescription(defaultDomainDescription);
      defaultDomain.setDataNodes(datanodeIds);
      defaultDomain.setStoragePools(storagePools);
      defaultDomain.setLastUpdateTime(System.currentTimeMillis());

      String defaultPoolName = "DefaultStoragePool";
      String defaultPoolDescription = "default storage pool for use";
      Multimap<Long, Long> archivesInDataNode = Multimaps
          .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
      StoragePool defaultStoragePool = new StoragePool();
      defaultStoragePool.setPoolId(defaultPoolId);
      defaultStoragePool.setDomainId(defaultDomainId);
      defaultStoragePool.setDomainName(defaultDomainName);
      defaultStoragePool.setName(defaultPoolName);
      defaultStoragePool.setDescription(defaultPoolDescription);
      defaultStoragePool.setStrategy(StoragePoolStrategy.Capacity);
      defaultStoragePool.setArchivesInDataNode(archivesInDataNode);
      defaultStoragePool.setLastUpdateTime(System.currentTimeMillis());

      // init datanodeIds and archivesInDatanode
      List<InstanceMetadataThrift> datanodes = listArchivesResponse.getInstanceMetadata();
      for (InstanceMetadataThrift datanode : datanodes) {
        long datanodeId = datanode.getInstanceId();
        datanodeIds.add(datanodeId);
        for (ArchiveMetadataThrift archive : datanode.getArchiveMetadata()) {
          long archiveId = archive.getArchiveId();
          archivesInDataNode.put(datanodeId, archiveId);
        }
      }

      // create default domain
      CreateDomainRequest createDomainRequest = new CreateDomainRequest();
      createDomainRequest.setRequestId(RequestIdBuilder.get());
      createDomainRequest.setDomain(RequestResponseHelper.buildDomainThriftFrom(defaultDomain));
      createDomainRequest.setAccountId(request.getAccountId());
      try {
        createDomain(createDomainRequest);
      } catch (TException e) {
        logger.error("Caught an exception", e);
        throw filterTexception(e);
      } catch (Exception e) {
        logger.error("create default domain caught an exception", e);
        throw new TException(e);
      }
      // create default storage pool
      CreateStoragePoolRequestThrift createStoragePoolRequest =
          new CreateStoragePoolRequestThrift();
      createStoragePoolRequest.setRequestId(RequestIdBuilder.get());
      createStoragePoolRequest
          .setStoragePool(
              RequestResponseHelper.buildThriftStoragePoolFrom(defaultStoragePool, null));
      createStoragePoolRequest.setAccountId(request.getAccountId());
      try {
        createStoragePool(createStoragePoolRequest);
      } catch (TException e) {
        logger.error("Caught an exception", e);
        throw filterTexception(e);
      } catch (Exception e) {
        logger.error("create default storage pool caught an exception", e);
        throw new TException(e);
      }
    } catch (TException e) {
      logger.error("Caught an exception", e);
      throw filterTexception(e);
    } catch (Exception e) {
      logger.error("caught an exception", e);
      throw new TException(e);
    }
    logger.warn("createDefaultDomainAndStoragePool response: {}", response);
    return response;
  }

  /**** other  end.  *****/

  @Override
  public GetDriverConnectPermissionResponseThrift getDriverConnectPermission(
      GetDriverConnectPermissionRequestThrift request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      NetworkErrorExceptionThrift,
      TException {

    //check the instance status
    checkInstanceStatus("getDriverConnectPermission");
    logger.warn("getDriverConnectPermission request: {}", request);

    GetDriverConnectPermissionResponseThrift response;

    // get driver container
    Instance driverContainer = instanceStore.get(new InstanceId(request.getDriverContainerId()));
    EndPoint endPoint = driverContainer.getEndPoint();
    try {
      DriverContainerServiceBlockingClientWrapper clientWrapper = driverContainerClientFactory
          .build(endPoint, 2000);
      // send request
      response = clientWrapper.getClient().getDriverConnectPermission(request);
    } catch (GenericThriftClientFactoryException e) {
      logger.error("catch an exception", e);
      throw new NetworkErrorExceptionThrift().setDetail(e.getMessage());
    } catch (ServiceHavingBeenShutdownThrift e) {
      logger.error("caught an exception", e);
      throw e;
    } catch (TException e) {
      logger.error("Caught an exception", e);
      throw filterTexception(e);
    } catch (Exception e) {
      logger.error("Caught an exception", e);
      throw new TException(e);
    }

    logger.warn("{}", response);
    return response;
  }

  @Override
  public SaveOperationLogsToCsvResponse saveOperationLogsToCsv(
      SaveOperationLogsToCsvRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      PermissionNotGrantExceptionThrift,
      AccountNotFoundExceptionThrift, TException {

    //check the instance status
    checkInstanceStatus("saveOperationLogsToCSV");

    logger.warn("saveOperationLogsToCSV request: {}", request);
    securityManager.hasPermission(request.getAccountId(), "saveOperationLogsToCSV");

    List<Operation> operationList = operationStore.getAllOperation();
    if (request.isSetAccountName()) {
      operationList = operationList.stream()
          .filter(o -> o.getAccountName().contains(request.getAccountName()))
          .collect(Collectors.toList());
    }
    if (request.isSetOperationType()) {
      operationList = operationList.stream()
          .filter(o -> o.getOperationType().name().equals(request.getOperationType()))
          .collect(Collectors.toList());
    }
    if (request.isSetTargetType()) {
      operationList = operationList.stream()
          .filter(o -> o.getTargetType().name().equals(request.getTargetType()))
          .collect(Collectors.toList());
    }
    if (request.isSetTargetName()) {
      operationList = operationList.stream()
          .filter(
              o -> (o.getOperationObject().contains(request.getTargetName()) || o.getTargetName()
                  .contains(request.getTargetName()))).collect(Collectors.toList());
    }
    if (request.isSetStatus()) {
      operationList = operationList.stream()
          .filter(o -> (o.getStatus().name().equals(request.getStatus())))
          .collect(Collectors.toList());
    }
    if (request.isSetStartTime()) {
      operationList = operationList.stream()
          .filter(o -> o.getStartTime() >= (request.getStartTime()))
          .collect(Collectors.toList());
    }
    if (request.isSetEndTime()) {
      operationList = operationList.stream().filter(o -> o.getEndTime() <= (request.getEndTime()))
          .collect(Collectors.toList());
    }

    operationList.sort(new Comparator<Operation>() {
      @Override
      public int compare(Operation o1, Operation o2) {
        return -Long.compare(o1.getStartTime(), o2.getStartTime());
      }
    });

    StringBuilder sb = new StringBuilder();
    String crlf = System.getProperty("line.separator", "\n");
    SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    sb.append("\"用户名\"").append(",").append("\"操作类型\"").append(",").append("\"目标类型\"")
        .append(",")
        .append("\"操作对象\"").append(",").append("\"状态\"").append(",").append("\"开始时间\"")
        .append(",")
        .append("\"结束时间\"").append(crlf);

    for (Operation operation : operationList) {

      OperationType operationType = operation.getOperationType();
      String operationTypeCnName = "";
      switch (operationType) {
        case CREATE:
          operationTypeCnName = "创建";
          break;
        case DELETE:
          operationTypeCnName = "删除";
          break;
        case EXTEND:
          operationTypeCnName = "扩展";
          break;
        case CYCLE:
          operationTypeCnName = "回收";
          break;
        case LAUNCH:
          operationTypeCnName = "挂载";
          break;
        case UMOUNT:
          operationTypeCnName = "卸载";
          break;
        case APPLY:
          operationTypeCnName = "应用";
          break;
        case CANCEL:
          operationTypeCnName = "取消";
          break;
        case MODIFY:
          operationTypeCnName = "修改";
          break;
        case LOGIN:
          operationTypeCnName = "登录";
          break;
        case LOGOUT:
          operationTypeCnName = "登出";
          break;
        case RESET:
          operationTypeCnName = "重置";
          break;
        case ONLINE:
          operationTypeCnName = "挂载";
          break;
        case OFFLINE:
          operationTypeCnName = "卸载";
          break;
        case ASSIGN:
          operationTypeCnName = "分配";
          break;
        case MIGRATE:
          operationTypeCnName = "重构";
          break;
        default:
          break;
      }

      TargetType targetType = operation.getTargetType();
      String targetTypeCnName = "";
      switch (targetType) {
        case DOMAIN:
          targetTypeCnName = "域";
          break;
        case STORAGEPOOL:
          targetTypeCnName = "存储池";
          break;
        case VOLUME:
          targetTypeCnName = "卷";
          break;
        case DISK:
          targetTypeCnName = "磁盘";
          break;
        case SERVICE:
          targetTypeCnName = "服务";
          break;
        case ACCESSRULE:
          targetTypeCnName = "客户机";
          break;
        case USER:
          targetTypeCnName = "用户";
          break;
        case DRIVER:
          targetTypeCnName = "驱动";
          break;
        case ROLE:
          targetTypeCnName = "角色";
          break;
        case PASSWORD:
          targetTypeCnName = "密码";
          break;
        case QOS:
          targetTypeCnName = "QOS";
          break;
        case RESOURCE:
          targetTypeCnName = "资源";
          break;
        case SERVERNODE:
          targetTypeCnName = "服务器节点";
          break;
        default:
          break;
      }

      sb.append("\"").append(operation.getAccountName()).append("\"").append(",").append("\"")
          .append(operationTypeCnName).append("\"").append(",").append("\"")
          .append(targetTypeCnName)
          .append("\"").append(",");

      //some error operation not have the operationObject, when operation do by restful
      String operationObject = operation.getOperationObject();
      String targetName = operation.getTargetName();

      if (operationObject == null) {
        operationObject = "";
        logger.warn("find the operationObject is null, in operation:{}, set to empty", operation);
      }

      if (targetName == null) {
        targetName = "";
        logger.warn("find the targetName is null, in operation:{}, set to empty", operation);
      }

      if (operationObject.isEmpty() || targetName.isEmpty()) {
        sb.append("\"").append(operationObject).append(targetName).append("\"")
            .append(",");
      } else {
        sb.append("\"").append(operationObject).append("(").append(targetName)
            .append(")").append("\"").append(",");
      }

      OperationStatus operationStatus = operation.getStatus();
      String operationStatusCnName = "";
      switch (operationStatus) {
        case SUCCESS:
          operationStatusCnName = "成功";
          break;
        case ACTIVITING:
          operationStatusCnName = "进行中";
          break;
        case FAILED:
          operationStatusCnName = "失败";
          break;
        default:
          break;
      }
      sb.append("\"").append(operationStatusCnName).append("\"").append(",").append("\"")
          .append(format.format(new Date(operation.getStartTime()))).append("\"").append(",")
          .append("\"")
          .append(format.format(new Date(operation.getEndTime()))).append("\"").append(crlf);
    }
    SaveOperationLogsToCsvResponse response = new SaveOperationLogsToCsvResponse();
    response.setRequestId(request.getRequestId());
    String csvFileString = sb.toString();
    try {
      response.setCsvFile(csvFileString.getBytes("UTF-8"));
    } catch (UnsupportedEncodingException e) {
      logger.warn("caught an exception.", e);
      throw new UnsupportedEncodingExceptionThrift().setDetail(e.getMessage());
    }
    logger.warn("saveOperationLogsToCSV response: {}", response);
    return response;
  }

  /******    InstanceMaintenance begin.  **********/
  @Override
  public InstanceMaintainResponse markInstanceMaintenance(InstanceMaintainRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      PermissionNotGrantExceptionThrift,
      AccountNotFoundExceptionThrift, EndPointNotFoundExceptionThrift,
      TooManyEndPointFoundExceptionThrift,
      NetworkErrorExceptionThrift, TException {

    //check the instance status
    checkInstanceStatus("markInstanceMaintenance");
    logger.warn("markInstanceMaintenance request: {}", request);

    securityManager.hasPermission(request.getAccountId(), "markInstanceMaintenance");
    final InstanceMaintainResponse response = new InstanceMaintainResponse(request.getRequestId());

    InstanceMaintenanceInformation informationInDb = instanceMaintenanceStore
        .getById(request.getInstanceId());
    InstanceMaintenanceInformation newInfo;
    if (informationInDb == null || System.currentTimeMillis() >= informationInDb.getEndTime()) {
      newInfo = new InstanceMaintenanceInformation(request.getInstanceId(),
          System.currentTimeMillis(),
          request.getDurationInMinutes() * 60 * 1000L, request.getIp());
    } else {
      newInfo = new InstanceMaintenanceInformation(request.getInstanceId(),
          informationInDb.getStartTime(),
          informationInDb.getDuration() + request.getDurationInMinutes() * 60 * 1000L,
          informationInDb.getIp());
    }

    instanceMaintenanceStore.save(newInfo);
    logger.warn("markInstanceMaintenance response: {}", response);
    return response;
  }

  /******    InstanceMaintenance begin . **********/

  @Override
  public CancelMaintenanceResponse cancelInstanceMaintenance(CancelMaintenanceRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      PermissionNotGrantExceptionThrift,
      AccountNotFoundExceptionThrift, EndPointNotFoundExceptionThrift,
      TooManyEndPointFoundExceptionThrift,
      NetworkErrorExceptionThrift, TException {

    //check the instance status
    checkInstanceStatus("cancelInstanceMaintenance");
    logger.warn("cancelInstanceMaintenance request: {}", request);
    securityManager.hasPermission(request.getAccountId(), "cancelInstanceMaintenance");

    CancelMaintenanceResponse response = new CancelMaintenanceResponse(request.getRequestId());
    InstanceMaintenanceInformation newInfo = instanceMaintenanceStore
        .getById(request.getInstanceId());
    instanceMaintenanceStore.deleteById(request.getInstanceId());

    logger.warn("cancelInstanceMaintenance response: {}", response);
    return response;
  }

  @Override
  public ListInstanceMaintenancesResponse listInstanceMaintenances(
      ListInstanceMaintenancesRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift {

    //check the instance status
    checkInstanceStatus("listInstanceMaintenances");
    logger.warn("listInstanceMaintenances request: {}", request);

    ListInstanceMaintenancesResponse response = new ListInstanceMaintenancesResponse();
    response.setRequestId(request.getRequestId());
    List<InstanceMaintenanceThrift> instanceMaintenanceThrifts = new ArrayList<>();
    response.setInstanceMaintenances(instanceMaintenanceThrifts);
    List<InstanceMaintenanceInformation> infos = instanceMaintenanceStore.listAll();
    if (request.isSetInstanceIds()) {
      infos.removeIf(i -> !request.getInstanceIds().contains(i.getInstanceId()));
    }
    for (InstanceMaintenanceInformation info : infos) {
      if (info != null) {
        if (System.currentTimeMillis() < info.getEndTime()) {
          InstanceMaintenanceThrift instanceMaintenanceThrift = new InstanceMaintenanceThrift();
          instanceMaintenanceThrift.setInstanceId(info.getInstanceId());
          instanceMaintenanceThrift.setStartTime(info.getStartTime());
          instanceMaintenanceThrift.setEndTime(info.getEndTime());
          instanceMaintenanceThrift.setCurrentTime(System.currentTimeMillis());
          instanceMaintenanceThrift.setDuration(info.getDuration());
          instanceMaintenanceThrift.setIp(info.getIp());
          instanceMaintenanceThrifts.add(instanceMaintenanceThrift);
        } else {
          instanceMaintenanceStore.delete(info);
        }
      }
    }
    logger.warn("listInstanceMaintenances response: {}", response);
    return response;
  }

  /******    Roles begin . **********/
  @Override
  public CreateRoleResponse createRole(CreateRoleRequest request)
      throws ServiceIsNotAvailableThrift, ServiceHavingBeenShutdownThrift,
      CreateRoleNameExistedExceptionThrift, PermissionNotGrantExceptionThrift,
      AccountNotFoundExceptionThrift,
      TException {

    //check the instance status
    checkInstanceStatus("createRole");
    logger.warn("createRole request: {}", request);

    securityManager.hasPermission(request.getAccountId(), "createRole");

    Role createdRole;
    try {
      createdRole = securityManager
          .createRole(request.getRoleName(), request.getDescription(), request.getApiNames(), false,
              false);
    } catch (TException e) {
      logger.error("caught an exception", e);
      throw e;
    } catch (Exception e) {
      logger.error("caught an exception", e);
      throw new TException(e);
    }

    CreateRoleResponse response = new CreateRoleResponse(request.getRequestId(),
        createdRole.getId());
    buildEndOperationWrtCreateRoleAndSaveToDb(request.getAccountId(), request.getRoleName());
    logger.warn("createRole response: {}", response);
    return response;
  }

  @Override
  public AssignRolesResponse assignRoles(AssignRolesRequest request)
      throws ServiceIsNotAvailableThrift, ServiceHavingBeenShutdownThrift,
      PermissionNotGrantExceptionThrift,
      AccountNotFoundExceptionThrift, CrudSuperAdminAccountExceptionThrift, TException {
    //check the instance status
    checkInstanceStatus("assignRoles");

    logger.warn("assignRoles request: {}", request);
    securityManager.hasPermission(request.getAccountId(), "assignRoles");

    long assignedToAccountId = request.getAssignedAccountId();
    List<Role> assignedRoles;
    try {
      assignedRoles = securityManager.assignRoles(assignedToAccountId, request.getRoleIds());
    } catch (TException e) {
      logger.error("caught an exception", e);
      throw e;
    } catch (Exception e) {
      logger.error("caught an exception", e);
      throw new TException(e);
    }
    List<String> assignedRoleNames = new ArrayList<>();
    for (Role role : assignedRoles) {
      assignedRoleNames.add(role.getName());
    }

    AssignRolesResponse response = new AssignRolesResponse(request.getRequestId());
    AccountMetadata account = securityManager.getAccountById(request.getAssignedAccountId());
    buildOperationAndSaveToDb(request.getAccountId(), null, OperationType.ASSIGN, TargetType.ROLE,
        account.getAccountName(), assignedRoleNames.stream().collect(Collectors.joining(", ")),
        OperationStatus.SUCCESS, 0L, null, null);
    logger.warn("assignRoles response: {}", response);
    return response;
  }

  @Override
  public DeleteRolesResponse deleteRoles(DeleteRolesRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      DeleteRoleExceptionThrift,
      PermissionNotGrantExceptionThrift, AccountNotFoundExceptionThrift, TException {

    //check the instance status
    checkInstanceStatus("deleteRoles");
    logger.warn("deleteRoles request: {}", request);

    securityManager.hasPermission(request.getAccountId(), "deleteRoles");

    Set<Long> rolesToDelete = request.getRoleIds();
    Set<Long> deletedRoleIds = new HashSet<>();
    List<String> deletedRoleNames = new ArrayList<>();
    boolean exceptionCaught = false;
    Map<String, String> failedRoleId2Cause = new HashMap<>();
    String failedCause;
    for (Long roleId : rolesToDelete) {
      Role role = null;
      try {
        role = securityManager.getRoleById(roleId);
        if (role == null) {
          throw new RoleNotExistedExceptionThrift();
        }
        Role deletedRole = securityManager.deleteRole(roleId);
        deletedRoleIds.add(deletedRole.getId());
        deletedRoleNames.add(deletedRole.getName());
      } catch (RoleNotExistedExceptionThrift e) {
        exceptionCaught = true;
        failedCause = "ERROR_RoleNotExistedException";
        logger.warn(failedCause);
        failedRoleId2Cause.put(roleId.toString(), failedCause);
      } catch (CrudBuiltInRoleExceptionThrift e) {
        exceptionCaught = true;
        failedCause = "ERROR_CRUDBuiltInRoleException";
        logger.warn(failedCause);
        failedRoleId2Cause.put(role.getName(), failedCause);
      } catch (RoleIsAssignedToAccountsExceptionThrift e) {
        exceptionCaught = true;
        failedCause = "ERROR_RoleIsAssignedToAccountsException";
        logger.warn(failedCause);
        failedRoleId2Cause.put(role.getName(), failedCause);
      }
    }
    if (exceptionCaught) {
      DeleteRoleExceptionThrift deleteRoleExceptionThrift = new DeleteRoleExceptionThrift(
          failedRoleId2Cause);
      logger.warn("Delete role caught exception: ", deleteRoleExceptionThrift);
      throw deleteRoleExceptionThrift;
    }

    DeleteRolesResponse deleteRolesResponse = new DeleteRolesResponse(RequestIdBuilder.get(),
        deletedRoleIds);
    buildEndOperationWrtDeleteRoleAndSaveToDb(request.getAccountId(),
        deletedRoleNames.stream().collect(Collectors.joining(", ")));
    logger.warn("deleteRoles response: {}", deleteRolesResponse);
    return deleteRolesResponse;
  }

  @Override
  public UpdateRoleResponse updateRole(UpdateRoleRequest request)
      throws ServiceIsNotAvailableThrift, ServiceHavingBeenShutdownThrift,
      RoleNotExistedExceptionThrift,
      PermissionNotGrantExceptionThrift, CrudBuiltInRoleExceptionThrift,
      AccountNotFoundExceptionThrift,
      TException {

    //check the instance status
    checkInstanceStatus("updateRole");
    logger.warn("updateRole request: {}", request);

    securityManager.hasPermission(request.getAccountId(), "updateRole");

    long roleId = request.getRoleId();
    String roleName = request.getRoleName();
    String description = request.getDescription();
    Role updatedRole;
    try {
      updatedRole = securityManager
          .updateRole(roleId, roleName, description, request.getApiNames());
    } catch (RoleNotExistedExceptionThrift e) {
      logger.error("caught an exception", e);
      throw e;
    } catch (TException e) {
      logger.error("caught an exception", e);
      throw e;
    } catch (Exception e) {
      logger.error("caught an exception", e);
      throw new TException(e);
    }

    UpdateRoleResponse response = new UpdateRoleResponse(request.getRequestId());
    buildEndOperationWrtModifyRoleAndSaveToDb(request.getAccountId(), updatedRole.getName());
    logger.warn("updateRole response: {}", response);
    return response;
  }

  /****** Roles end.  **********/

  @Override
  public ListApisResponse listApis(ListApisRequest request)
      throws ServiceIsNotAvailableThrift, ServiceHavingBeenShutdownThrift, TException {

    //check the instance status
    checkInstanceStatus("listAPIs");
    logger.warn("listAPIs request: {}", request);

    //        securityManager.hasPermission(request.getAccountId(), "listAPIs");

    ListApisResponse response = new ListApisResponse();
    response.setRequestId(request.getRequestId());
    Set<ApiToAuthorizeThrift> allApis = new HashSet<>();
    response.setApis(allApis);
    List<ApiToAuthorize> apis = securityManager.listApis();
    for (ApiToAuthorize api : apis) {
      allApis.add(RequestResponseHelper.buildApiToAuthorizeThrift(api));
    }
    logger.warn("listAPIs response: {}", response);
    return response;
  }

  @Override
  public ListRolesResponse listRoles(ListRolesRequest request)
      throws ServiceIsNotAvailableThrift, ServiceHavingBeenShutdownThrift,
      PermissionNotGrantExceptionThrift,
      AccountNotFoundExceptionThrift, TException {

    //check the instance status
    checkInstanceStatus("listRoles");
    logger.warn("listRoles request: {}", request);

   

    ListRolesResponse response = new ListRolesResponse();
    response.setRequestId(request.getRequestId());
    Set<RoleThrift> allRoles = new HashSet<>();
    response.setRoles(allRoles);
    List<Role> roles = securityManager.listRoles();
    if (request.isSetListRoleIds()) {
      for (Role role : roles) {
        if (!role.isSuperAdmin() && request.getListRoleIds().contains(role.getId())) {
          allRoles.add(RequestResponseHelper.buildRoleThrift(role));
        }
      }
    } else {
      for (Role role : roles) {
        if (!role.isSuperAdmin()) {
          allRoles.add(RequestResponseHelper.buildRoleThrift(role));
        }
      }
    }

    logger.warn("listRoles response: {}", response);
    return response;
  }

  /****** Account begin. *******/
  @Override
  public CreateAccountResponse createAccount(CreateAccountRequest request)
      throws AccessDeniedExceptionThrift, InvalidInputExceptionThrift,
      AccountAlreadyExistsExceptionThrift,
      ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      PermissionNotGrantExceptionThrift,
      AccountNotFoundExceptionThrift, TException {

    //check the instance status
    checkInstanceStatus("createAccount");

    logger.warn("createAccount request: {}", request);
    securityManager.hasPermission(request.getAccountId(), "createAccount");

    AccountMetadata account;
    try {
      account = securityManager
          .createAccount(request.getAccountName(), request.getPassword(),
              request.getAccountType().name(),
              request.getCreatingAccountId(), request.getRoleIds());
    } catch (TException e) {
      logger.error("caught an exception", e);
      throw e;
    } catch (Exception e) {
      logger.error("caught an exception", e);
      throw new TException(e);
    }
    CreateAccountResponse response = new CreateAccountResponse(account.getAccountId(),
        request.getAccountName());
    buildEndOperationWrtCreateUserAndSaveToDb(request.getAccountId(), account.getAccountName());
    logger.warn("createAccount response: {}", response);
    return response;
  }

  @Override
  public DeleteAccountsResponse deleteAccounts(DeleteAccountsRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      AccountNotFoundExceptionThrift,
      AccessDeniedExceptionThrift, PermissionNotGrantExceptionThrift,
      DeleteLoginAccountExceptionThrift,
      TException {

    //check the instance status
    checkInstanceStatus("deleteAccounts");
    logger.warn("deleteAccounts request: {}", request);

    securityManager.hasPermission(request.getAccountId(), "deleteAccounts");

    //todo:delete the not exit Account
    if (request.getDeletingAccountIds().isEmpty()) {
      logger.warn("deleteAccounts, can not find any Account to delete");
      throw new AccountNotFoundExceptionThrift();
    }

    Set<Long> deletedAccountIds = new HashSet<>();
    List<String> deletedAccountNames = new ArrayList();
    final DeleteAccountsResponse response = new DeleteAccountsResponse(request.getRequestId(),
        deletedAccountIds);
    boolean deleteLoginAccount = false;
    for (long accountId : request.getDeletingAccountIds()) {
      try {
        if (request.getAccountId() != accountId) {
          AccountMetadata account = securityManager.deleteAccount(accountId);
          if (null != account) {
            deletedAccountIds.add(account.getAccountId());
            deletedAccountNames.add(account.getAccountName());
          }
        } else {
          deleteLoginAccount = true;
        }
      } catch (CrudSuperAdminAccountExceptionThrift e) {
        logger.warn(e.getDetail());
        logger.warn("Cannot delete super admin account.");
      }
    }

    if (deleteLoginAccount) {
      DeleteLoginAccountExceptionThrift deleteLoginAccountExceptionThrift =
          new DeleteLoginAccountExceptionThrift();
      logger.warn("Cannot delete the login account.", deleteLoginAccountExceptionThrift);
      throw deleteLoginAccountExceptionThrift;
    }

    //may be the request deletingAccountIds is empty
    if (!deletedAccountNames.isEmpty()) {
      buildEndOperationWrtDeleteUserAndSaveToDb(request.getAccountId(),
          deletedAccountNames.stream().collect(Collectors.joining(", ")));
    }
    logger.warn("deleteAccounts response: {}", response);
    return response;
  }

  @Override
  public UpdatePasswordResponse updatePassword(UpdatePasswordRequest request)
      throws AccessDeniedExceptionThrift, InvalidInputExceptionThrift, ServiceIsNotAvailableThrift,
      AccountNotFoundExceptionThrift, ServiceHavingBeenShutdownThrift,
      OlderPasswordIncorrectExceptionThrift,
      TException {

    //check the instance status
    checkInstanceStatus("updatePassword");
    logger.warn("updateAccount request: {}", request);

    //        securityManager.hasPermission(request.getAccountId(), "updatePassword");

    AccountMetadata account;
    try {
      account = securityManager
          .updatePassword(request.getAccountName(), request.getNewPassword(),
              request.getOldPassword(),
              request.getAccountName());
    } catch (TException e) {
      logger.error("caught an exception", e);
      throw e;
    } catch (Exception e) {
      logger.error("caught an exception", e);
      throw new TException(e);
    }

    buildOperationAndSaveToDb(request.getAccountId(), null, OperationType.MODIFY,
        TargetType.PASSWORD,
        account.getAccountName(), "", OperationStatus.SUCCESS, 0L, null, null);
    UpdatePasswordResponse response = new UpdatePasswordResponse(account.getAccountId());
    logger.warn("updateAccount response: {}", response);
    return response;
  }

  @Override
  public ListAccountsResponse listAccounts(ListAccountsRequest request)
      throws AccessDeniedExceptionThrift, InvalidInputExceptionThrift,
      ServiceHavingBeenShutdownThrift,
      ServiceIsNotAvailableThrift, AccountNotFoundExceptionThrift, TException {

    //check the instance status
    checkInstanceStatus("listAccounts");

    logger.warn("listAccounts request: {}", request);

    //        securityManager.hasPermission(request.getAccountId(), "listAccounts");
    ListAccountsResponse response = new ListAccountsResponse();
    try {
      List<AccountMetadataThrift> accountsThrift = new ArrayList<>();
      response.setAccounts(accountsThrift);
      if (request.isSetListAccountIds()) {
        Set<Long> listAccountIds = request.getListAccountIds();
        for (AccountMetadata account : securityManager.listAccounts(request.getAccountId())) {
          if (listAccountIds.contains(account.getAccountId())) {
            accountsThrift.add(RequestResponseHelper.buildAccountMetadataThriftFrom(account));
          }
        }
      } else {
        for (AccountMetadata account : securityManager.listAccounts(request.getAccountId())) {
          accountsThrift.add(RequestResponseHelper.buildAccountMetadataThriftFrom(account));
        }
      }
    } catch (TException e) {
      logger.error("caught an exception", e);
      throw e;
    } catch (Exception e) {
      logger.error("caught an exception", e);
      throw new TException(e);
    }

    logger.warn("listAccounts response: {}", response);
    return response;
  }

  @Override
  public ListResourcesResponse listResources(ListResourcesRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      PermissionNotGrantExceptionThrift,
      AccountNotFoundExceptionThrift, TException {

    //check the instance status
    checkInstanceStatus("listResources");

    logger.warn("listResources request: {}", request);

   

    List<ResourceThrift> resourcesThrift = new ArrayList<>();
    ListResourcesResponse response = new ListResourcesResponse(request.getRequestId(),
        resourcesThrift);
    if (request.isSetListResourceIds()) {
      Set<Long> listResourcesIds = request.getListResourceIds();
      for (PyResource resource : securityManager.listResources()) {
        if (listResourcesIds.contains(resource.getResourceId())) {
          if (resource.getResourceName().contains(MOVE_VOLUME_APPEND_STRING_FOR_ORIGINAL_VOLUME)) {
            continue;
          }
          resourcesThrift.add(RequestResponseHelper.buildResourceThrift(resource));
        }
      }
    } else {
      for (PyResource resource : securityManager.listResources()) {
        if (resource.getResourceName().contains(MOVE_VOLUME_APPEND_STRING_FOR_ORIGINAL_VOLUME)) {
          continue;
        }
        resourcesThrift.add(RequestResponseHelper.buildResourceThrift(resource));
      }
    }
    logger.warn("listResources response: {}", response);
    return response;
  }

  @Override
  public AssignResourcesResponse assignResources(AssignResourcesRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      PermissionNotGrantExceptionThrift,
      AccountNotFoundExceptionThrift, TException {

    //check the instance status
    checkInstanceStatus("assignResources");
    logger.warn("assignResources request: {}", request);

    securityManager.hasPermission(request.getAccountId(), "assignResources");

    List<String> assignedResources;
    try {
      assignedResources = securityManager
          .assignResources(request.getTargetAccountId(), request.getResourceIds());
    } catch (TException e) {
      logger.error("caught an exception", e);
      throw e;
    } catch (Exception e) {
      logger.error("caught an exception", e);
      throw new TException(e);
    }
    AccountMetadata account = securityManager.getAccountById(request.getTargetAccountId());
    buildOperationAndSaveToDb(request.getAccountId(), null, OperationType.ASSIGN,
        TargetType.RESOURCE,
        account.getAccountName(), assignedResources.stream().collect(Collectors.joining(", ")),
        OperationStatus.SUCCESS, 0L, null, null);
    AssignResourcesResponse response = new AssignResourcesResponse(request.getRequestId());
    logger.warn("assignResources response: {}", response);
    return response;
  }

  /**
   * login to sysytem.
   **/
  @Override
  public AuthenticateAccountResponse authenticateAccount(AuthenticateAccountRequest request)
      throws AuthenticationFailedExceptionThrift, ServiceHavingBeenShutdownThrift,
      ServiceIsNotAvailableThrift,
      TException {

    //check the instance status
    checkInstanceStatus("authenticateAccount");

    logger.warn("authenticateAccount request: {}", request);

    AccountMetadata account = securityManager
        .authenticateAccount(request.getAccountName(), request.getPassword());
    AuthenticateAccountResponse response = new AuthenticateAccountResponse();
    if (account != null) {
      logger.warn("get the account:{}", account);
      response.setAccountId(account.getAccountId());
      response.setAccountName(account.getAccountName());
      response.setAccountType(AccountTypeThrift.valueOf(account.getAccountType()));
      List<RoleThrift> roleThrifts = new ArrayList<>();
      List<ApiToAuthorizeThrift> apiToAuthorizeThrifts = new ArrayList<>();
      response.setRoles(roleThrifts);
      response.setApis(apiToAuthorizeThrifts);
      for (Role role : account.getRoles()) {
        roleThrifts.add(RequestResponseHelper.buildRoleThrift(role));
      }
      try {
        for (ApiToAuthorize api : securityManager
            .collectPermissionsByAccountId(account.getAccountId())) {
          apiToAuthorizeThrifts.add(RequestResponseHelper.buildApiToAuthorizeThrift(api));
        }
      } catch (AccountNotFoundExceptionThrift e) {
        throw new AuthenticationFailedExceptionThrift();
      }

      //Separate login and load volume operation,so remove loadVolumesInDB() function and create
      // new function loadVolulme()
      //loadVolumesInDB(account.getAccountId());
      buildEndOperationWrtUserLoginAndSaveToDb(account.getAccountId(), account.getAccountName());
      logger.warn("authenticateAccount response: {}", response);
      return response;
    }
    throw new AuthenticationFailedExceptionThrift();
  }
  /*     Account end    *******/

  /**
   * set the Account Password to default.
   ****/
  @Override
  public ResetAccountPasswordResponse resetAccountPassword(ResetAccountPasswordRequest request)
      throws InvalidInputExceptionThrift, AccessDeniedExceptionThrift,
      AccountNotFoundExceptionThrift,
      ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      PermissionNotGrantExceptionThrift,
      TException {

    //check the instance status
    checkInstanceStatus("resetAccountPassword");
    logger.warn("resetAccountPassword request: {}", request);

    securityManager.hasPermission(request.getAccountId(), "resetAccountPassword");

    AccountMetadata account;
    try {
      account = securityManager.resetAccountPassword(request.getTargetAccountId());
    } catch (TException e) {
      logger.error("caught an exception", e);
      throw e;
    } catch (Exception e) {
      logger.error("caught an exception", e);
      throw new TException(e);
    }

    buildEndOperationWrtResetPasswordAndSaveToDb(request.getAccountId(), account.getAccountName());
    ResetAccountPasswordResponse response = new ResetAccountPasswordResponse();
    response.setRequestId(request.getRequestId());
    response.setPassword(DEFAULT_PASSWORD);
    logger.warn("resetAccountPassword response: {}", response);
    return response;
  }

  @Override
  public LogoutResponse logout(LogoutRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      AccountNotFoundExceptionThrift,
      TException {

    //check the instance status
    checkInstanceStatus("logout");

    logger.warn("logout request: {}", request);
    AccountMetadata accountMetadata = securityManager.getAccountById(request.getAccountId());
    if (accountMetadata == null) {
      throw new AccountNotFoundExceptionThrift()
          .setDetail("Cannot find account with id " + request.getAccountId());
    }
    buildEndOperationWrtUserLogoutAndSaveToDb(request.getAccountId(),
        accountMetadata.getAccountName());
    LogoutResponse response = new LogoutResponse(request.getRequestId());
    logger.warn("logout response: {}", response);
    return response;
  }

  /* only for test  begin ***/
  public void shutdownForTest() {
    this.shutDownFlag = true;
  }

  /**
   * for test Operation.
   ***/
  @Override
  public CleanOperationInfoResponse cleanOperationInfo(CleanOperationInfoRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift {
    //check the instance status
    checkInstanceStatus("cleanOperationInfo");
    logger.warn("test cleanOperationInfo request: {}", request);

    List<Operation> operationList = operationStore.getAllOperation();
    for (Operation operation : operationList) {
      operationStore.deleteOperation(operation.getOperationId());
    }

    CleanOperationInfoResponse response = new CleanOperationInfoResponse();
    logger.warn("test cleanOperationInfo response: {}", response);
    return response;
  }

  /**
   * for test distributeVolume.
   ***/
  @Override
  public InstanceIncludeVolumeInfoResponse getInstanceIncludeVolumeInfo(
      InstanceIncludeVolumeInfoRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift {
    //check the instance status
    checkInstanceStatus("getInstanceIncludeVolumeInfo");
    logger.warn("test getInstanceIncludeVolumeInfo request: {}", request);

    Map<Long, Set<Long>> instanceToVolumeInfo = new HashMap<>();
    Map<Long, InstanceToVolumeInfo> instanceToVolumeInfoMap = instanceIncludeVolumeInfoManger
        .getInstanceToVolumeInfoMap();

    for (Map.Entry<Long, InstanceToVolumeInfo> entry : instanceToVolumeInfoMap.entrySet()) {
      Set<Long> volumeList = new HashSet<>();
      volumeList.addAll(entry.getValue().getVolumeInfo());

      instanceToVolumeInfo.put(entry.getKey(), volumeList);
    }

    InstanceIncludeVolumeInfoResponse response = new InstanceIncludeVolumeInfoResponse();
    response.setInstanceIncludeVolumeInfo(instanceToVolumeInfo);
    logger.warn("test getInstanceIncludeVolumeInfo response: {}", response);
    return response;
  }

  /**
   * for test Equilibrium volume.
   ***/
  @Override
  public EquilibriumVolumeResponse setEquilibriumVolumeStartOrStop(EquilibriumVolumeRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift {
    //check the instance status
    checkInstanceStatus("setEquilibriumVolumeStartOrStop");
    logger.warn("test setEquilibriumVolumeStart request: {}", request);

    //set status
    instanceVolumeInEquilibriumManger.setStartTest(request.isStatus());
    EquilibriumVolumeResponse response = new EquilibriumVolumeResponse();
    response.setRequestId(request.getRequestId());
    logger.warn("test setEquilibriumVolumeStartOrStop response: {}", response);
    return response;
  }

  /**
   * add the develop info.
   ****/

  private void updateServerNodeDiskInfo(ServerNodeThrift serverNode, String diskName,
      String status, long accountId)
      throws TException {
    String diskId = null;
    String diskLightStatus = status;
    Set<HardDiskInfoThrift> diskInfoSet = serverNode.getDiskInfoSet();
    for (HardDiskInfoThrift diskInfo : diskInfoSet) {
      if (diskName.equals(diskInfo.getName())) {
        diskId = diskInfo.getId();
        logger
            .debug("server networkCardInfo is: {},diskInfo is: {}", serverNode.getNetworkCardInfo(),
                diskInfo);
        break;
      }
    }

    try {
      if (diskId != null && diskLightStatus != null) {
        UpdateDiskLightStatusByIdRequestThrift request =
            new UpdateDiskLightStatusByIdRequestThrift();
        request.setRequestId(RequestIdBuilder.get());
        request.setDiskId(diskId);
        request.setStatus(status);
        logger.warn("update disk status started, diskId:{}, diskLightStatus:{}", diskId,
            diskLightStatus);
        UpdateDiskLightStatusByIdResponseThrift response = updateDiskLightStatusById(request);
        logger.warn("update disk status successed, diskId:{}, diskLightStatus:{}, responseId:{}",
            diskId,
            diskLightStatus, response.getResponseId());
      } else {
        logger.warn("update disk status failed, diskId:{}, diskLightStatus:{}", diskId,
            diskLightStatus);
      }
    } catch (TException e) {
      logger.error("Caught an exception", e);
      throw e;
    } catch (Exception e) {
      logger.error("Caught an exception", e);
      throw new TException(e);
    }

  }

  private void updateServerNodeDiskInfo(ServerNode serverNode, String diskName, String status,
      long accountId) {
    String diskId = null;
    String diskLightStatus = status;
    Set<DiskInfo> diskInfoSet = serverNode.getDiskInfoSet();
    for (DiskInfo diskInfo : diskInfoSet) {
      if (diskName.equals(diskInfo.getName())) {
        diskId = diskInfo.getId();
        logger
            .debug("server networkCardInfo is: {},diskInfo is: {}", serverNode.getNetworkCardInfo(),
                diskInfo);
        break;
      }
    }

    if (diskId != null && diskLightStatus != null) {
      logger.warn("update disk status started, diskId:{}, diskLightStatus:{}", diskId,
          diskLightStatus);
      diskInfoStore.updateDiskInfoLightStatusById(diskId, status);
    } else {
      logger.warn("update disk status failed, diskId:{}, diskLightStatus:{}", diskId,
          diskLightStatus);
    }
  }

  @Override
  public UpdateDiskLightStatusByIdResponseThrift updateDiskLightStatusById(
      UpdateDiskLightStatusByIdRequestThrift request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {
    logger.warn("begin updateDiskLightStatusById, request is: {}", request);
    String diskId = request.getDiskId();
    String status = request.getStatus();

    diskInfoStore.updateDiskInfoLightStatusById(diskId, status);

    UpdateDiskLightStatusByIdResponseThrift response =
        new UpdateDiskLightStatusByIdResponseThrift();
    response.setResponseId(request.getRequestId());
    logger.warn("end updateDiskLightStatusById, response is: {}", response);
    return response;
  }

  /**
   * check server node is net sub health.
   */
  public boolean isServerNodeNetSubHealth(ServerNode serverNode) {
   
   
    Set<String> netCardIpSet = new HashSet<>(
        NodeInfo.parseNetCardInfoToGetIps(serverNode.getNetworkCardInfo()));
    //find systemdaemon endpoint(if servernode have no systemdaemon service, it also cannot set
    // net sub health)
    EndPoint systemDaemonEndPoint = null;
    //Set<Instance> instanceSet = instanceStore.getAll(PyService.SYSTEMDAEMON.getServiceName());
    Set<Instance> instanceSet = new HashSet<>();
    for (Instance instance : instanceSet) {
      Map<PortType, EndPoint> instanceEndPoints = instance.getEndPoints();
      for (EndPoint endPointTemp : instanceEndPoints.values()) {
        if (netCardIpSet.contains(endPointTemp.getHostName())) {
          systemDaemonEndPoint = endPointTemp;
          break;
        }
      }
    }
    if (systemDaemonEndPoint != null) {
      return determinDataNodeIsNetworkSubhealthy(systemDaemonEndPoint.getHostName());
    }

    return false;
  }

  private boolean determinDataNodeIsNetworkSubhealthy(String datanodeHostname) {
    //Instance systemDaemon = instanceStore.getByHostNameAndServiceName(datanodeHostname,
    //PyService.SYSTEMDAEMON.getServiceName());
    Instance systemDaemon = null;
    if (systemDaemon == null) {
      return false;
    }
    boolean networkSubhealthy = systemDaemon.isNetSubHealth();
    if (networkSubhealthy) {
      logger.warn("datanode:{} is network subhealthy now, mark as SEPARATED status",
          datanodeHostname);
    }
    return networkSubhealthy;
  }


  
  public LaunchDriverResponseThrift beginLaunchDriver(LaunchDriverRequest request)
      throws VolumeNotFoundExceptionThrift, VolumeNotAvailableExceptionThrift,
      NotRootVolumeExceptionThrift,
      VolumeBeingDeletedExceptionThrift, TooManyDriversExceptionThrift,
      ServiceHavingBeenShutdownThrift,
      DriverTypeConflictExceptionThrift, VolumeWasRollbackingExceptionThrift,
      GetPydDriverStatusExceptionThrift, DriverUnmountingExceptionThrift,
      SystemMemoryIsNotEnoughThrift,
      DriverAmountAndHostNotFitThrift, DriverHostCannotUseThrift, DriverIsUpgradingExceptionThrift,
      PermissionNotGrantExceptionThrift, AccountNotFoundExceptionThrift,
      DriverTypeIsConflictExceptionThrift,
      ExistsDriverExceptionThrift, DriverNameExistsExceptionThrift,
      VolumeLaunchMultiDriversExceptionThrift,
      NetworkErrorExceptionThrift, GetScsiClientExceptionThrift, NoEnoughPydDeviceExceptionThrift,
      ConnectPydDeviceOperationExceptionThrift, CreateBackstoresOperationExceptionThrift,
      CreateLoopbackOperationExceptionThrift, CreateLoopbackLunsOperationExceptionThrift,
      GetScsiDeviceOperationExceptionThrift, ScsiDeviceIsLaunchExceptionThrift,
      InfocenterServerExceptionThrift, ServiceIsNotAvailableThrift, ScsiVolumeLockExceptionThrift,
      TException {

    String volumeName = null;
    List<EndPoint> endPoints = new ArrayList<>();
    if (this.shutDownFlag) {
      throw new ServiceHavingBeenShutdownThrift();
    }

    if (InstanceStatus.SUSPEND == appContext.getStatus()) {
      logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
      throw new ServiceIsNotAvailableThrift().setDetail("I am suspend");
    }

    logger.warn("beginLaunchDriver request: {}", request);
    logger.warn("beginLaunchDriver for volume :{}, in client :{}", request.getVolumeId(),
        request.getScsiIp());

    securityManager.hasPermission(request.getAccountId(), "launchDriver");
    securityManager.hasRightToAccess(request.getAccountId(), request.getVolumeId());

    long volumeId = request.getVolumeId();
    String scsiIp = request.getScsiIp();

    VolumeMetadata volumeMetadata = volumeStore.getVolume(volumeId);

    //check the Operation
    checkExceptionForOperation(volumeMetadata, OperationFunctionType.launchDriver);

    LockForSaveVolumeInfo.VolumeLockInfo lock = lockForSaveVolumeInfo
        .getVolumeLockInfo(volumeId, LockForSaveVolumeInfo.BusySource.DRIVER_LAUNCHING);

    boolean isException = false;
    boolean needOperation = true;
    String driverName = "Scsi"; //just for Scsi client

    try {
      // this section used for transition from NBD to PYD
      if (request.getDriverType() != DriverTypeThrift.NBD.getValue()) {
        logger.warn("when beginLaunchDriver, the input tDriverType :{}, error",
            request.getDriverType());
        throw new TException();
      }

      //check for launch for scsi
      long driverContainerIdScsi = 0;
      ScsiClient scsiClient = scsiClientStore
          .getScsiClientInfoByVolumeIdAndSnapshotId(scsiIp, volumeId, request.getSnapshotId());
      if (scsiClient != null && ScsiDriverDescription.Normal.name()
          .equals(scsiClient.getStatusDescription())) {
        //if have Normal Description, the scis lauch ok, else the task again
        logger.error(
            "when launchDriver for scsi, find the volume launch in client :{}, please unmount and"
                + " launch again",
            scsiIp);
        throw new ScsiDeviceIsLaunchExceptionThrift();
      }

      //get the driverContainerIdScsi
      List<ScsiClient> scsiClientList = scsiClientStore.getScsiClientByIp(scsiIp);
      if (scsiClientList != null && !scsiClientList.isEmpty()) {
        // the volume id is 0, set the
        for (ScsiClient client : scsiClientList) {
          if (client.getScsiClientInfo().getVolumeId() == 0) {
            driverContainerIdScsi = client.getDriverContainerIdScsi();
            break;
          }
        }
      } else {
        logger.error("beginLaunchDriver, can not find the scsi client :{}", scsiIp);
        throw new TException();
      }

      if (driverContainerIdScsi == 0) {
        logger.error("beginLaunchDriver, get the driverContainerIdScsi error");
        throw new TException();
      }

      synchronized (this) {

        VolumeMetadata volume;
        List<DriverMetadata> existingDriverList;
        try {
          VolumeMetadataAndDrivers volumeAndDriverInfo = volumeInformationManger
              .getDriverContainVolumes(request.getVolumeId(), request.getAccountId(), true);

          volume = volumeAndDriverInfo.getVolumeMetadata();
          volumeName = volume.getName();
          existingDriverList = volumeAndDriverInfo.getDriverMetadatas();
        } catch (VolumeNotFoundException e) {
          logger.error("when launchDriver, can not find the volume :{}", request.getVolumeId());
          throw new VolumeNotFoundExceptionThrift();
        }

        if (volume == null) {
          logger.error("No such volume with id {}", request.getVolumeId());
          throw new VolumeNotFoundExceptionThrift();
        }

        if (!volume.isVolumeAvailable()) {
          logger.error("Status of volume {} is {}, launchDriver fail!", volume,
              volume.getVolumeStatus());
          throw new VolumeNotAvailableExceptionThrift();
        }

        if (!volume.isEnableLaunchMultiDrivers()) {
          if (!existingDriverList.isEmpty() || request.getDriverAmount() != 1) {
            logger.warn("Volume is not allowed to launch more than one driver.");
            throw new VolumeLaunchMultiDriversExceptionThrift();
          }
        }

        LaunchDriverResponseThrift response = new LaunchDriverResponseThrift();
        response.setRequestId(request.getRequestId());

        List<DriverContainerCandidate> driverContainerCandidateList = null;
        try {
         
         
         

          driverContainerCandidateList = allocDriverContainer(volumeId, request.getDriverAmount(),
              true);
        } catch (TooManyDriversExceptionThrift e) {
          logger.error("Not enough available driver container to launch driver for volume {}",
              request.getVolumeId(), e);
          throw e;
        }

        
        List<EndPoint> endPointList = new ArrayList<>();

        // check if amount driver container candidates is enough, only the
        if (driverContainerCandidateList.size() < request.getDriverAmount()) {
          logger.error("there is not enough DriverContainer: {}", driverContainerCandidateList);
          throw new TooManyDriversExceptionThrift();
        }

       
        
        int networkErrorCount = 0;
        int systemMemoryNotEnoughErrorCount = 0;
        int driverIsUpgrading = 0;
        Queue<DriverContainerCandidate> candidateReuseQueue = new LinkedList<>();
        for (int i = 0; i < request.getDriverAmount(); ++i) {
          while (!driverContainerCandidateList.isEmpty()) {
            DriverContainerCandidate driverContainerCandidate = driverContainerCandidateList
                .remove(0);

            EndPoint eps = new EndPoint(driverContainerCandidate.getHostName(),
                driverContainerCandidate.getPort());
            logger.warn("get the driverContainer end point :{}, which will launch Driver", eps);
            DriverContainerServiceBlockingClientWrapper dcClientWrapper;
            LaunchDriverRequestThrift launchDriverRequest = new LaunchDriverRequestThrift();
            launchDriverRequest.setRequestId(request.getRequestId());
            launchDriverRequest.setAccountId(request.getAccountId());
            launchDriverRequest.setVolumeId(request.getVolumeId());
            launchDriverRequest.setSnapshotId(request.getSnapshotId());
            launchDriverRequest
                .setDriverType(DriverTypeThrift.findByValue(request.getDriverType()));
            launchDriverRequest.setDriverAmount(request.getDriverAmount());

            //set the DriverName
            if (request.getScsiIp() != null) {
              launchDriverRequest.setDriverName(String.valueOf(RequestIdBuilder.get()));
            }

            logger.warn("Sending launch driver request {} to {}, driver container service list {}",
                launchDriverRequest, eps, driverContainerCandidate);

            long driverContainerId = 0;
            boolean thisTimeLaunchDriverStatus = false;

            //get the connect scsi client ip
            EndPoint scsiClientIoHost = null;
            Set<Instance> driverContainerInstances = instanceStore
                .getAll(PyService.DRIVERCONTAINER.getServiceName(), InstanceStatus.HEALTHY);
            logger.warn("Existing driver containers {}", driverContainerInstances);
            for (Instance instance : driverContainerInstances) {
              if (instance.getEndPoint().getHostName().equals(scsiIp)) {
                //get the io port
                scsiClientIoHost = instance.getEndPointByServiceName(PortType.IO);
                break;
              }
            }
            logger.warn("get the scsi client :{}, IO port :{}", scsiIp, scsiClientIoHost);

            if (scsiClientIoHost == null) {
              logger.warn("get the scsi client :{}, io port error", scsiIp);
              throw new GetScsiClientExceptionThrift();
            }

            //create volume access
            String connectScsiClientHostName = scsiClientIoHost.getHostName();
            long ruleId = RequestIdBuilder.get();
            final List<VolumeAccessRuleThrift> accessRules = new ArrayList<>();
            VolumeAccessRuleThrift volumeAccessRuleThrift = new VolumeAccessRuleThrift();
            volumeAccessRuleThrift.setRuleId(ruleId);
            volumeAccessRuleThrift.setIncomingHostName(connectScsiClientHostName);
            volumeAccessRuleThrift.setPermission(AccessPermissionTypeThrift.READWRITE);
            accessRules.add(volumeAccessRuleThrift);

            CreateVolumeAccessRulesRequest createVolumeAccessRules =
                new CreateVolumeAccessRulesRequest();
            createVolumeAccessRules.setRequestId(RequestIdBuilder.get());
            createVolumeAccessRules.setAccountId(request.getAccountId());
            createVolumeAccessRules.setAccessRules(accessRules);
            createVolumeAccessRules.setForScsiClienIsSet(true);
            try {
              createVolumeAccessRules(createVolumeAccessRules);
              logger.warn("createVolumeAccessRules for volume :{} ok, the ruleId is :{}", volumeId,
                  ruleId);
            } catch (VolumeAccessRuleDuplicateThrift e) {
              ruleId = Long.valueOf(e.getDetail()).longValue();
              logger.warn(
                  "createVolumeAccessRules for volume :{}, there is have same access id :{}, so "
                      + "not create",
                  ruleId, volumeId);
            } catch (TException e) {
              logger.error("Caught an exception createVolumeAccessRules", e);
              throw new CreateVolumeAccessRulesExceptionThrift();
            }

            try {
              //ApplyVolumeAccess
              ApplyVolumeAccessRuleOnVolumesRequest applyVolumeAccessRuleOnVolumesRequest =
                  new ApplyVolumeAccessRuleOnVolumesRequest();

              List<Long> volumeIds = new ArrayList<>();
              volumeIds.add(volumeId);
              applyVolumeAccessRuleOnVolumesRequest.setRequestId(RequestIdBuilder.get());
              applyVolumeAccessRuleOnVolumesRequest.setAccountId(request.getAccountId());
              applyVolumeAccessRuleOnVolumesRequest.setVolumeIds(volumeIds);
              applyVolumeAccessRuleOnVolumesRequest.setRuleId(ruleId);
              applyVolumeAccessRuleOnVolumesRequest.setCommit(true);
              applyVolumeAccessRuleOnVolumes(applyVolumeAccessRuleOnVolumesRequest);
              logger.warn("applyVolumeAccessRuleOnVolumes for volume :{} ok", volumeId);
            } catch (TException e) {
              logger.error("Caught an exception applyVolumeAccessRuleOnVolumes", e);
              throw new ApplyVolumeAccessRuleExceptionThrift();
            }

            //begin launchDriver
            try {
              dcClientWrapper = driverContainerClientFactory.build(eps, 100000);
              LaunchDriverResponseThrift launchDriverResponseThrift = dcClientWrapper.getClient()
                  .launchDriver(launchDriverRequest);
              Validate.isTrue(launchDriverResponseThrift.isSetRelatedDriverContainerIds());
              Validate.isTrue(launchDriverResponseThrift.getRelatedDriverContainerIdsSize() == 1);

              driverContainerId = launchDriverResponseThrift.getRelatedDriverContainerIds()
                  .iterator()
                  .next();
              response.addToRelatedDriverContainerIds(driverContainerId);
              logger.warn("Succeed to launch driver {}!", driverContainerCandidate);
              endPointList.add(eps);
              endPoints.add(eps);

              //Launch ok, wait for status ok
              thisTimeLaunchDriverStatus = true;
            } catch (SystemMemoryIsNotEnoughThrift e) {
              ++systemMemoryNotEnoughErrorCount;
              logger.warn("failed to launch driver on {}, system is out of memory, try next!", eps);
            } catch (ExistsDriverExceptionThrift e) {
              logger.warn("{}, driver container already receive the same request!", eps);
              candidateReuseQueue.add(driverContainerCandidate);
            } catch (DriverIsUpgradingExceptionThrift e) {
              ++driverIsUpgrading;
              logger.warn("Host {}, driver is upgrading, try next!", eps);
            } catch (DriverTypeIsConflictExceptionThrift e) {
              logger.warn("Driver type is conflict {}!", request.getDriverType());
            } catch (DriverNameExistsExceptionThrift e) {
              logger.warn("DriverName exists or null!");
              throw e;
            } catch (TException e) {
              ++networkErrorCount;
              logger.warn("Failed to launch driver on {}, because of Exception: {}, try next!", eps,
                  e);
            } catch (Exception e) {
              logger.warn("Failed to launch driver on {}, because of Exception: {}, try next!", eps,
                  e);
            }

            //add for the scsi driver
            if (thisTimeLaunchDriverStatus) {
              //save the pyd DriverContainerId
              scsiClientStore
                  .updateScsiPydDriverContainerId(scsiIp, volumeId, request.getSnapshotId(),
                      driverContainerId);

              //check the Driver Status
              String hostName = driverContainerCandidate.getHostName();
              int waitTime = 10;
              boolean launchDriverOk = false;
              while (waitTime-- > 0) {
                ListAllDriversResponse listAllDriversResponse = null;
                try {
                  ListAllDriversRequest listAllDriversRequest = new ListAllDriversRequest();
                  listAllDriversRequest.setRequestId(RequestIdBuilder.get());
                  listAllDriversRequest.setVolumeId(volumeId);
                  logger.warn("listAllDriversRequest: {}", listAllDriversRequest);
                  // TODO add launch driver operation
                  listAllDriversResponse = listAllDrivers(listAllDriversRequest);
                } catch (TException e) {
                  logger.warn("for scsi listAllDriversRequest, get exception :", e);
                }

                //wait to get status
                if (listAllDriversResponse == null) {
                  try {
                    Thread.sleep(2000);
                    logger.warn("wait for listAllDrivers next time");
                  } catch (InterruptedException e) {
                    logger.error("Caught an exception", e);
                  }
                } else {
                  List<DriverMetadataThrift> driverMetadataThriftList = listAllDriversResponse
                      .getDriverMetadatasthrift();
                  logger.warn("get the all DriverMetadata info :{}", driverMetadataThriftList);
                  for (DriverMetadataThrift driverMetadataThrift : driverMetadataThriftList) {
                    if (driverMetadataThrift.getVolumeId() == volumeId
                        && driverMetadataThrift.getSnapshotId() == request.getSnapshotId()
                        && driverMetadataThrift.getDriverContainerId() == driverContainerId
                        && driverMetadataThrift.getDriverStatus()
                        == DriverStatusThrift.LAUNCHED) {
                      launchDriverOk = true;
                      logger.warn("find the driver status ok, the driver:{} ",
                          driverMetadataThrift);
                      break;
                    }
                  }
                }

                //break while
                if (launchDriverOk) {
                  break;
                } else {
                  try {
                    Thread.sleep(2000);
                  } catch (InterruptedException e) {
                    logger.error("Caught an exception", e);
                  }
                }
              }

              //check get status ok or not
              if (!launchDriverOk) {

                //umount Driver
                logger.error("the Driver not launch ok, so umount it");
                unMountDriverWithPydForScsi(hostName, volumeId, request.getAccountId(),
                    driverContainerId, request.getSnapshotId());
                throw new GetPydDriverStatusExceptionThrift();
              }

              //check the
              DriverContainerServiceBlockingClientWrapper dcClientWrapperScsi = null;
              try {
                EndPoint epToScsi = new EndPoint(scsiIp, driverContainerCandidate.getPort());
                dcClientWrapperScsi = driverContainerClientFactory
                    .build(epToScsi, 100000);
              } catch (GenericThriftClientFactoryException e) {
                //umount the driver
                logger.error("build scsi Wrapper error, so umount it");
                unMountDriverWithPydForScsi(driverContainerCandidate.getHostName(), volumeId,
                    request.getAccountId(), driverContainerId, request.getSnapshotId());
                throw new GetScsiClientExceptionThrift();
              }

              MountScsiDeviceRequest mountScsiDeviceRequest = new MountScsiDeviceRequest();
              mountScsiDeviceRequest.setRequestId(RequestIdBuilder.get());
              mountScsiDeviceRequest.setVolumeId(volumeId);
              mountScsiDeviceRequest.setSnapshotId(request.getSnapshotId());
              mountScsiDeviceRequest.setDriverIp(driverContainerCandidate.getHostName());
              logger.warn("begin to mountScsiDevice, the request is :{}", mountScsiDeviceRequest);

              try {
                MountScsiDeviceResponse mountScsiDeviceResponse = dcClientWrapperScsi.getClient()
                    .mountScsiDevice(mountScsiDeviceRequest);
                logger.warn("get the mountScsiDeviceResponse: {}", mountScsiDeviceResponse);

                //about the save, some error
                if (mountScsiDeviceResponse != null) {
                  if (mountScsiDeviceResponse.getDriverContainerIdForScsi()
                      != driverContainerIdScsi) {
                    logger.warn(
                        "some error in the id different, the driverContainerIdScsi :{}, the "
                            + "for scsi :{}", driverContainerIdScsi,
                        mountScsiDeviceResponse.getDriverContainerIdForScsi());
                  }

                  ScsiClientInfo scsiClientInfo = new ScsiClientInfo(scsiIp, volumeId,
                      request.getSnapshotId());
                  ScsiClient scsiClientSave = new ScsiClient(scsiClientInfo, driverContainerId,
                      driverContainerIdScsi, ScsiDriverDescription.Normal.name(),
                      DriverStatusThrift.LAUNCHING.name(), TaskType.LaunchDriver.name());
                  scsiClientStore.saveOrUpdateScsiClientInfo(scsiClientSave);

                  logger.warn("mountScsiDevice is ok");
                  //break while
                  break;
                }

              } catch (NoEnoughPydDeviceExceptionThrift e) {
                logger.warn("when mountScsiDevice, find error :", e);
                unMountDriverWithPydForScsi(driverContainerCandidate.getHostName(), volumeId,
                    request.getAccountId(), driverContainerId, request.getSnapshotId());
                throw new NoEnoughPydDeviceExceptionThrift();
              } catch (ConnectPydDeviceOperationExceptionThrift e) {
                logger.warn("when mountScsiDevice, find error :", e);
                unMountDriverWithPydForScsi(driverContainerCandidate.getHostName(), volumeId,
                    request.getAccountId(), driverContainerId, request.getSnapshotId());
                throw new ConnectPydDeviceOperationExceptionThrift();
              } catch (CreateBackstoresOperationExceptionThrift e) {
                logger.warn("when mountScsiDevice, find error :", e);
                unMountDriverWithPydForScsi(driverContainerCandidate.getHostName(), volumeId,
                    request.getAccountId(), driverContainerId, request.getSnapshotId());
                throw new CreateBackstoresOperationExceptionThrift();
              } catch (CreateLoopbackOperationExceptionThrift e) {
                logger.warn("when mountScsiDevice, find error :", e);
                unMountDriverWithPydForScsi(driverContainerCandidate.getHostName(), volumeId,
                    request.getAccountId(), driverContainerId, request.getSnapshotId());
                throw new CreateLoopbackOperationExceptionThrift();
              } catch (CreateLoopbackLunsOperationExceptionThrift e) {
                logger.warn("when mountScsiDevice, find error :", e);
                unMountDriverWithPydForScsi(driverContainerCandidate.getHostName(), volumeId,
                    request.getAccountId(), driverContainerId, request.getSnapshotId());
                throw new CreateLoopbackLunsOperationExceptionThrift();
              } catch (GetScsiDeviceOperationExceptionThrift e) {
                logger.warn("when mountScsiDevice, find error :", e);
                unMountDriverWithPydForScsi(driverContainerCandidate.getHostName(), volumeId,
                    request.getAccountId(), driverContainerId, request.getSnapshotId());
                throw new GetScsiDeviceOperationExceptionThrift();
              } catch (TException e) {
                logger.warn("when mountScsiDevice, find error :", e);
                unMountDriverWithPydForScsi(driverContainerCandidate.getHostName(), volumeId,
                    request.getAccountId(), driverContainerId, request.getSnapshotId());
                throw new GetScsiClientExceptionThrift();
              }
            }
          }

          while (!candidateReuseQueue.isEmpty()) {
            driverContainerCandidateList.add(candidateReuseQueue.poll());
          }
        }

        if (endPointList.size() < request.getDriverAmount()) {
          if (networkErrorCount > 0) {
            logger.error("Failed to launch all drivers because of network error happened.");
            throw new TTransportException(
                "Failed to launch all drivers because of network error happened.");
          } else if (systemMemoryNotEnoughErrorCount > 0) {
            logger.error(
                "Failed to launch all drivers because some driver containers are lack of memory "
                    + "to launch the drivers.");
            throw new SystemMemoryIsNotEnoughThrift();
          } else if (driverIsUpgrading > 0) {
            logger.error("Failed to launch all drivers because some drivers are upgrading.");
            throw new DriverIsUpgradingExceptionThrift();
          } else {
            logger.error("Failed to launch all drivers because some other reasons.");
            throw new TooManyDriversExceptionThrift();
          }
        }

        if (endPointList == null || endPointList.isEmpty()) {
          logger.error("end point is empty, may be something wrong happened");
        }
        logger
            .warn("beginLaunchDriver, with scsi :{}, with volume :{}, return response: {}", scsiIp,
                volumeId,
                response);
        return response;
      }
    } catch (TException e) {
      logger.error("launchDriver for scsi :{} with volume :{} end, which caught exception", scsiIp,
          volumeId, e);
      if (e instanceof ScsiDeviceIsLaunchExceptionThrift) {
        needOperation = false;
      }
      isException = true;
      throw filterTexception(e);
    } finally {
      try {
        logger.warn("when launchDriver caught a exception or not, the value: {}", isException);
        if (needOperation) {
          if (isException) {
            buildFailedOperationWrtLaunchDriverAndSaveToDb(request.getAccountId(),
                request.getVolumeId(),
                OperationType.LAUNCH, volumeName, driverName, endPoints);
          } else {
            buildActiveOperationWrtLaunchDriverAndSaveToDb(request.getAccountId(),
                request.getVolumeId(),
                OperationType.LAUNCH, volumeName, driverName, endPoints);
          }
        }
      } finally {
        lock.setVolumeOperation(LockForSaveVolumeInfo.BusySource.LOCK_WAITING);
        lock.unlock();
      }
    }
  }

  @Override
  public LaunchDriverResponseThrift launchDriverForScsi(LaunchScsiDriverRequestThrift request)
      throws ServiceHavingBeenShutdownThrift, AccountNotFoundExceptionThrift,
      PermissionNotGrantExceptionThrift, ServiceIsNotAvailableThrift,
      ScsiClientIsNotOkExceptionThrift,
      TException {
    if (this.shutDownFlag) {
      throw new ServiceHavingBeenShutdownThrift();
    }

    if (InstanceStatus.SUSPEND == appContext.getStatus()) {
      logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
      throw new ServiceIsNotAvailableThrift().setDetail("I am suspend");
    }

    logger.warn("launchDriverForScsi request: {}", request);

    securityManager.hasPermission(request.getAccountId(), "launchDriver");
    final Map<Long, Integer> volumesForLaunch = request.getVolumesForLaunch();
    Map<Long, Long> volumeToTaskInfo = new HashMap<>();
    String scsiIp = request.getScsiIp();

    /* check the scsi client status is ok or not **/
    boolean scsiClientStatusOk = false;
    Set<Instance> driverContainerInstances = instanceStore
        .getAll(PyService.DRIVERCONTAINER.getServiceName());
    for (Instance instance : driverContainerInstances) {
      if (instance.getEndPoint().getHostName().equals(scsiIp)
          && instance.getStatus() == InstanceStatus.HEALTHY) {
        scsiClientStatusOk = true;
      }
    }

    //client not ok
    if (!scsiClientStatusOk) {
      logger.error("when launchDriverForScsi, the scsi client :{} status not ok", scsiIp);
      throw new ScsiClientIsNotOkExceptionThrift();
    }

    List<ScsiClient> scsiClientInfoGet = scsiClientStore.getScsiClientByIp(scsiIp);

    if (scsiClientInfoGet == null) {
      logger.error("launchDriverForScsi, the input ip is error");
      throw new TException();
    }

    /* get driverContainerIdScsi value **/
    long driverContainerIdScsi = 0;
    for (ScsiClient value : scsiClientInfoGet) {
      long volumeId = value.getScsiClientInfo().getVolumeId();
      if (volumeId == 0) {
        driverContainerIdScsi = value.getDriverContainerIdScsi();
        break;
      }
    }

    if (driverContainerIdScsi == 0) {
      logger.error("launchDriverForScsi, get the driverContainerIdScsi error");
      throw new TException();
    }

    /* begin create task **/
    LaunchDriverRequest launchDriverRequest = new LaunchDriverRequest();
    launchDriverRequest.setRequestId(request.getRequestId());
    launchDriverRequest.setAccountId(request.getAccountId());
    launchDriverRequest.setDriverType(request.getDriverType().getValue());
    launchDriverRequest.setDriverAmount(1);
    launchDriverRequest.setScsiIp(request.getScsiIp());

    for (Map.Entry<Long, Integer> entry : volumesForLaunch.entrySet()) {
      long volumeId = entry.getKey();
      int snapshotId = entry.getValue();

      //check the volume Launch or not
      ScsiClient scsiClient = scsiClientStore
          .getScsiClientInfoByVolumeIdAndSnapshotId(scsiIp, volumeId, snapshotId);
      if (scsiClient != null) {
        logger
            .error("when launch volume :{}, {}, which is in scsi client :{}", volumeId, snapshotId,
                scsiIp);
        continue;
      }

      //save
      ScsiClientInfo scsiClientInfo = new ScsiClientInfo(scsiIp, volumeId, snapshotId);
      ScsiClient scsiClientSave = new ScsiClient(scsiClientInfo, 0, driverContainerIdScsi,
          ScsiDriverDescription.BeginLaunching.name(), DriverStatusThrift.LAUNCHING.name(),
          TaskType.LaunchDriver.name());
      scsiClientStore.saveOrUpdateScsiClientInfo(scsiClientSave);

      launchDriverRequest.setVolumeId(volumeId);
      launchDriverRequest.setSnapshotId(snapshotId);

      //save task
      TaskRequestInfo taskRequestInfo = new TaskRequestInfo();
      taskRequestInfo.setTaskId(RequestIdBuilder.get());
      taskRequestInfo.setTaskType(TaskType.LaunchDriver.name());
      taskRequestInfo.setRequest(launchDriverRequest);
      taskRequestInfo.setTaskCreateTime(System.currentTimeMillis());

      try {
        taskStore.saveTask(taskRequestInfo);
        volumeToTaskInfo.put(volumeId, taskRequestInfo.getTaskId());
      } catch (IOException e) {
        logger.error("when launchDriverForScsi, save the launchDriverRequest :{} error",
            launchDriverRequest);
      }
    }

    LaunchDriverResponseThrift response = new LaunchDriverResponseThrift();
    response.setRequestId(request.getRequestId());
    logger.warn("launchDriverForScsi, the task info :{}", volumeToTaskInfo);
    logger.warn("launchDriverForScsi response: {}", response);
    return response;
  }

  private void unMountDriverWithPydForScsi(String hostName, long volumeId, long accountId,
      long driverContainerId,
      int snapshotId) {

    final List<DriverIpTargetThrift> driverIpTargetList = new ArrayList<>();
    DriverIpTargetThrift driverIpTargetThrift = new DriverIpTargetThrift();
    driverIpTargetThrift.setDriverType(DriverTypeThrift.NBD);
    driverIpTargetThrift.setDriverIp(hostName);
    driverIpTargetThrift.setDriverContainerId(driverContainerId);
    driverIpTargetThrift.setSnapshotId(snapshotId);
    driverIpTargetList.add(driverIpTargetThrift);

    UmountDriverRequestThrift umountDriverRequestThrift = new UmountDriverRequestThrift();
    umountDriverRequestThrift.setVolumeId(volumeId);
    umountDriverRequestThrift.setAccountId(accountId);
    umountDriverRequestThrift.setDriverIpTargetList(driverIpTargetList);
    logger.warn("begin to unMountDriverWithPydForScsi, the volume :{}, the request :{}", volumeId,
        umountDriverRequestThrift);
    try {
      umountDriver(umountDriverRequestThrift);
    } catch (TException e) {
      logger.warn("when unMountDriverWithPydForScsi for volume :{} in client :{}, find a error:",
          volumeId,
          hostName, e);
    }
  }

  public UmountDriverResponseThrift beginUmountDriver(UmountDriverRequest request)
      throws VolumeNotFoundExceptionThrift, ServiceHavingBeenShutdownThrift,
      FailedToUmountDriverExceptionThrift, ExistsClientExceptionThrift,
      DriverIsLaunchingExceptionThrift,
      SnapshotRollingBackExceptionThrift,
      DriverLaunchingExceptionThrift, DriverUnmountingExceptionThrift,
      DriverIsUpgradingExceptionThrift,
      NetworkErrorExceptionThrift, CanNotGetPydDriverExceptionThrift,
      DriverContainerIsIncExceptionThrift,
      PermissionNotGrantExceptionThrift, GetScsiClientExceptionThrift,
      AccountNotFoundExceptionThrift,
      ServiceIsNotAvailableThrift, InfocenterServerExceptionThrift, TException {

    boolean isException = false;
    String volumeName = "notFind";
    List<String> umountDriverNames = new ArrayList<>();
    // end point list used for operation save
    List<EndPoint> endPointList = new ArrayList<>();

    try {
      if (this.shutDownFlag) {
        throw new ServiceHavingBeenShutdownThrift();
      }
      if (InstanceStatus.SUSPEND == appContext.getStatus()) {
        logger
            .error("Refuse request from remote due to I am suspend, request content: {}", request);
        throw new ServiceIsNotAvailableThrift().setDetail("I am suspend");
      }

      logger.warn("beginUmountDriver request: {}", request);
      logger.warn("beginUmountDriver for volume :{}, in client :{}", request.getVolumeId(),
          request.getScsiClientIp());

      securityManager.hasPermission(request.getAccountId(), "umountDriver");
      securityManager.hasRightToAccess(request.getAccountId(), request.getVolumeId());

      long volumeId = request.getVolumeId();
      final String clientIp = request.getScsiClientIp();

      // this section for searching driver type name and volume name, used for operation save
      VolumeMetadata volume = null;
      List<DriverMetadata> driverMetadataList = new ArrayList<>();
      try {
        VolumeMetadataAndDrivers volumeAndDriverInfo = volumeInformationManger
            .getDriverContainVolumes(volumeId, request.getAccountId(), false);
        volume = volumeAndDriverInfo.getVolumeMetadata();
        driverMetadataList = volumeAndDriverInfo.getDriverMetadatas();
      } catch (VolumeNotFoundException e) {
        logger.error("umountDriverForScsi, get volume name, Caught an exception", e);
      }

      //begin unmount pyd driver, for operation
      if (volume != null) {
        volumeName = volume.getName();
      }

      umountDriverNames.add("Scsi");
      EndPoint endPoint = new EndPoint();
      endPoint.setHostName(clientIp);
      endPointList.add(endPoint);

      if (driverMetadataList.isEmpty()) {
        logger.warn("when beginUmountDriver for volume :{}, in client: {}, there is no driver",
            volumeId,
            clientIp);
        scsiClientStore
            .deleteScsiClientInfoByVolumeIdAndSnapshotId(clientIp, request.getVolumeId(), 0);
        UmountDriverResponseThrift response = new UmountDriverResponseThrift(
            request.getRequestId(), null);
        return response;
      }

      logger.warn("beginUmountDriver got drivers:{} binding with volume:{}", driverMetadataList,
          volumeId);
      //check the scsi client for umountDriver
      long driverContainerId = 0;

      //get pyd driverContainerId
      ScsiClient scsiClient = scsiClientStore
          .getScsiClientInfoByVolumeIdAndSnapshotId(clientIp, request.getVolumeId(), 0);

      if (scsiClient != null) {
        driverContainerId = scsiClient.getDriverContainerId();
      }

      if (driverContainerId == 0) {
        logger.warn(
            "umountDriver for scsi, can not get the pyd driverContainerId, so i think lauch "
                + "failed, can delete the info");
        scsiClientStore
            .deleteScsiClientInfoByVolumeIdAndSnapshotId(clientIp, request.getVolumeId(), 0);
        UmountDriverResponseThrift response = new UmountDriverResponseThrift(
            request.getRequestId(), null);
        return response;
      }

      //get the pyd driver ip
      String driverIp = null;
      Set<Instance> driverContainerInstances = instanceStore
          .getAll(PyService.DRIVERCONTAINER.getServiceName());
      logger.warn("Existing driver containers {}", driverContainerInstances);

      for (Instance instance : driverContainerInstances) {
        if (instance.getId().getId() == driverContainerId) {
          if (instance.getStatus() == InstanceStatus.HEALTHY) {
            driverIp = instance.getEndPoint().getHostName();
          }
        }
      }

      if (driverIp == null) {
        logger.warn("umountDriver, can not get the pyd driverIp");
        throw new CanNotGetPydDriverExceptionThrift();
      }

      //set the driverIp for umount pyd Driver
      final List<DriverIpTargetThrift> driverIpTargetList = new ArrayList<>();
      DriverIpTargetThrift driverIpTargetThrift = new DriverIpTargetThrift();
      driverIpTargetThrift.setDriverIp(driverIp);
      driverIpTargetThrift.setDriverType(DriverTypeThrift.NBD);
      driverIpTargetThrift.setDriverContainerId(driverContainerId);
      driverIpTargetThrift.setSnapshotId(0); // this time set 0
      driverIpTargetList.add(driverIpTargetThrift);

      //Umount to scsi
      UmountScsiDeviceRequest umountScsiDeviceRequest = new UmountScsiDeviceRequest();
      umountScsiDeviceRequest.setRequestId(RequestIdBuilder.get());
      umountScsiDeviceRequest.setDriverIp(driverIp);
      umountScsiDeviceRequest.setVolumeId(volumeId);
      umountScsiDeviceRequest.setSnapshotId(0); // current time, the SnapshotId set 0

      //get the scsi dri ip
      Instance driverContainersForScsi = null;
      Set<Instance> driverContainerInstancesForScsi = instanceStore
          .getAll(PyService.DRIVERCONTAINER.getServiceName(), InstanceStatus.HEALTHY);
      logger.warn("beginUmountDriver, Existing driver containers for umountScsi {}",
          driverContainerInstancesForScsi);

      for (Instance instance : driverContainerInstancesForScsi) {
        if (instance.getEndPoint().getHostName().equals(clientIp)) {
          driverContainersForScsi = instance;
          break;
        }
      }

      logger.warn("get the umountScsi ip is: {}", driverContainersForScsi);
      if (driverContainersForScsi == null) {
        logger.warn("umountDriver, can not get the driverContainersForScsi");
        throw new GetScsiClientExceptionThrift();
      }

      EndPoint scsiClientIoHost = driverContainersForScsi.getEndPoint();

      if (scsiClientIoHost == null) {
        logger.warn("umountDriver get the scsi client :{}, ip port error", clientIp);
        throw new GetScsiClientExceptionThrift();
      }

      //unmount scsi driver
      try {
        logger.warn(
            "umountDriver, in EndPoint :{} for volume :{}, get the umountScsiDeviceRequest is :{}",
            scsiClientIoHost, volumeId, umountScsiDeviceRequest);
        DriverContainerServiceBlockingClientWrapper dcClientWrapper = driverContainerClientFactory
            .build(scsiClientIoHost, 50000);
        UmountScsiDeviceResponse umountScsiDeviceResponse = dcClientWrapper.getClient()
            .umountScsiDevice(umountScsiDeviceRequest);
        logger
            .warn("umountScsiDevice ok,for driverIp :{} volume :{}", scsiClientIoHost.getHostName(),
                volumeId);

        //just for pyd dis
        Thread.sleep(2000);
      } catch (GenericThriftClientFactoryException e) {
        logger.warn("umountDriver, can not get the dcClientWrapper");
        throw new GetScsiClientExceptionThrift();
      } catch (InterruptedException e) {
        //for sleep
        logger.warn("umountDriver, get a error for sleep :", e);
      } catch (TException e) {
        logger.warn("umountDriver, get a error for umountScsiDevice:", e);
        throw new GetScsiClientExceptionThrift();
      }

      Set<Instance> driverContainers = new HashSet<>();
      Set<DriverIpTargetThrift> driverContainerNotFind = new HashSet<>();
      driverContainerInstances = instanceStore.getAll(PyService.DRIVERCONTAINER.getServiceName());

      logger.warn("umount to get pyd, Existing driver containers {}", driverContainerInstances);
      boolean driverContainerIsInc = false;
      for (DriverIpTargetThrift driverIpValue : driverIpTargetList) {
        boolean getDriverContainer = false;
        for (Instance instance : driverContainerInstances) {
          if (instance.getId().getId() == driverIpValue.getDriverContainerId()) {
            if (instance.getStatus() == InstanceStatus.HEALTHY) {
              driverContainers.add(instance);
            } else {
              driverContainerIsInc = true;
            }

            getDriverContainer = true;
          }
        }

        if (!getDriverContainer) {
          driverContainerNotFind.add(driverIpValue);
        }
      }

      logger.debug("Umount drivers on driver containers {}", driverContainers);
      for (Instance driverContainer : driverContainers) {
        try {
          final DriverContainerServiceBlockingClientWrapper dcClientWrapper =
              driverContainerClientFactory
                  .build(driverContainer.getEndPoint(), 50000);
          UmountDriverRequestThrift requestToOneDc = new UmountDriverRequestThrift();
          requestToOneDc.setRequestId(request.getRequestId());
          requestToOneDc.setAccountId(request.getAccountId());
          requestToOneDc.setVolumeId(volumeId);

          for (DriverIpTargetThrift driverOne : driverIpTargetList) {
            if (driverOne.getDriverContainerId() == driverContainer.getId().getId()) {
              requestToOneDc.addToDriverIpTargetList(driverOne);
            }
          }

          logger.warn("umountDriver request_ToOneDC:{} to driver container:{}", requestToOneDc,
              driverContainer.getEndPoint());
          UmountDriverResponseThrift umountDriverResponse = dcClientWrapper.getClient()
              .umountDriver(requestToOneDc);
          if (umountDriverResponse.isSetDriverIpTarget()) {
            List<DriverIpTargetThrift> allDrivers = requestToOneDc.getDriverIpTargetList();
            for (DriverIpTargetThrift failedDriver : umountDriverResponse.getDriverIpTarget()) {
              failedDriver.setDriverIp(driverContainer.getEndPoint().getHostName());
              allDrivers.remove(failedDriver);
            }
          }

          // notify info the driver is in removing status
          try {
            logger.warn("beginUmountDriver, markDriverStatus: {}",
                requestToOneDc.getDriverIpTargetList());
            markDriverStatus(request.getVolumeId(), requestToOneDc.getDriverIpTargetList());
          } catch (Exception e) {
            logger.warn("notify info center driver failed: markDriverStatusRequest", e);
          }

          //releaseDriverContainer
          synchronized (driverStore) {
            for (DriverIpTargetThrift driverIpTargetThrift1 : requestToOneDc
                .getDriverIpTargetList()) {
              DriverType driverType = DriverType
                  .valueOf(driverIpTargetThrift1.getDriverType().name());
              driverStore.delete(driverIpTargetThrift1.getDriverContainerId(), volumeId, driverType,
                  driverIpTargetThrift1.getSnapshotId());
            }
          }

        } catch (ExistsClientExceptionThrift e) {
          logger.error("beginUmountDriver, Caught an exception", e);
          throw e;
        } catch (DriverIsLaunchingExceptionThrift e) {
          logger.error("beginUmountDriver, Caught an exception", e);
          throw e;
        } catch (TTransportException e) {
          logger.error("beginUmountDriver, Caught an exception", e);
          throw new NetworkErrorExceptionThrift();
        } catch (GenericThriftClientFactoryException e) {
          logger.error("beginUmountDriver, Caught an exception", e);
          throw new NetworkErrorExceptionThrift();
        } catch (TException e) {
          logger.error("beginUmountDriver, Caught an exception", e);
          throw e;
        } catch (Exception e) {
          logger.error("Caught an exception when umount driver on {}", driverContainer, e);
          throw new TException(e);
        }
      }

      if (!driverContainerNotFind.isEmpty()) {
        logger.warn("when umountDriver, can not find the driverContainers{} for volume :{}",
            driverContainerNotFind, volumeId);
      }

      if (driverContainerIsInc) {
        throw new DriverContainerIsIncExceptionThrift();
      }

      //remove clientIp
      scsiClientStore.deleteScsiClientInfoByVolumeIdAndSnapshotId(clientIp, volumeId, 0);

      UmountDriverResponseThrift response = new UmountDriverResponseThrift(request.getRequestId(),
          null);
      logger.warn("beginUmountDriver response: {}", response);
      return response;

    } catch (TException e) {
      logger.error("beginUmountDriver, caught an exception", e);
      isException = true;
      throw filterTexception(e);
    } finally {
      logger.warn("beginUmountDriver have caught exception or not: {}", isException);
      if (isException) {
        buildEndOperationFailedScsiUmountDriverAndSaveToDb(request.getAccountId(),
            request.getVolumeId(),
            OperationType.UMOUNT, volumeName,
            umountDriverNames.stream().collect(Collectors.joining(", ")),
            endPointList);
      } else {
        buildEndOperationSuccessScsiUmountDriverAndSaveToDb(request.getAccountId(),
            request.getVolumeId(),
            OperationType.UMOUNT, volumeName,
            umountDriverNames.stream().collect(Collectors.joining(", ")),
            endPointList);
      }
    }
  }

  @Override
  public UmountScsiDriverResponseThrift umountDriverForScsi(UmountScsiDriverRequestThrift request)
      throws ServiceHavingBeenShutdownThrift, AccountNotFoundExceptionThrift,
      PermissionNotGrantExceptionThrift, ServiceIsNotAvailableThrift,
      ScsiClientIsNotOkExceptionThrift,
      TException {
    if (this.shutDownFlag) {
      throw new ServiceHavingBeenShutdownThrift();
    }

    if (InstanceStatus.SUSPEND == appContext.getStatus()) {
      logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
      throw new ServiceIsNotAvailableThrift().setDetail("I am suspend");
    }

    String scsiIp = request.getScsiClientIp();
    logger.warn("umountDriverForScsi request: {}", request);

    securityManager.hasPermission(request.getAccountId(), "umountDriver");
    Map<String, ScsiClientOperationExceptionThrift> umountDriverError = new HashMap<>();

    /* check the scsi client status is ok or not **/
    boolean scsiClientStatusOk = false;
    Set<Instance> driverContainerInstances = instanceStore
        .getAll(PyService.DRIVERCONTAINER.getServiceName());
    for (Instance instance : driverContainerInstances) {
      if (instance.getEndPoint().getHostName().equals(scsiIp)
          && instance.getStatus() == InstanceStatus.HEALTHY) {
        scsiClientStatusOk = true;
        break;
      }
    }

    //client not ok
    if (!scsiClientStatusOk) {
      logger.error("when umountDriverForScsi, the scsi client :{} status not ok", scsiIp);
      throw new ScsiClientIsNotOkExceptionThrift();
    }

    /* set the status to REMOVING, and save task **/

    Map<Long, Long> volumeToTaskInfo = new HashMap<>();
    UmountDriverRequest umountDriverRequest = new UmountDriverRequest();
    umountDriverRequest.setRequestId(request.getRequestId());
    umountDriverRequest.setAccountId(request.getAccountId());
    umountDriverRequest.setScsiClientIp(request.getScsiClientIp());

    Map<Long, Integer> volumesForUmount = request.getVolumesForUmount();
    for (Map.Entry<Long, Integer> entry : volumesForUmount.entrySet()) {
      long volumeId = entry.getKey();
      int snapshotId = entry.getValue();

      //set the scsi drivers status
      //check the volume Launch or not
      ScsiClient scsiClient = scsiClientStore
          .getScsiClientInfoByVolumeIdAndSnapshotId(scsiIp, volumeId, snapshotId);
      if (scsiClient == null) {
        logger.error("when umountDriverForScsi for volume :{}, {}, which is not in scsi client :{}",
            volumeId,
            snapshotId, scsiIp);
        continue;
      } else {
        logger.warn("umountDriverForScsi, get the info :{}", scsiClient.toString());
        //check the volume in launching or not, umounting or not
        if ((TaskType.LaunchDriver.name().equals(scsiClient.getDescriptionType())
            && ScsiDriverDescription.BeginLaunching.name()
            .equals(scsiClient.getStatusDescription()))
            //launching
            || (TaskType.UmountDriver.name().equals(scsiClient.getDescriptionType()) // or umounting
            && ScsiDriverDescription.BeginUnmounting.name()
            .equals(scsiClient.getStatusDescription()))) {

          //current volume in Launching or umounting
          VolumeMetadata volume = null;
          try {
            VolumeMetadataAndDrivers volumeAndDriverInfo = volumeInformationManger
                .getDriverContainVolumes(volumeId, request.getAccountId(), false);
            volume = volumeAndDriverInfo.getVolumeMetadata();
          } catch (VolumeNotFoundException e) {
            logger.error("umountDriverForScsi, get volume name, Caught an exception", e);
          }

          if (volume != null) {
            String volumeName = volume.getName();
            if (TaskType.LaunchDriver.name().equals(scsiClient.getDescriptionType())) {
              umountDriverError.put(volumeName,
                  ScsiClientOperationExceptionThrift.VolumeLaunchingExceptionThrift);
            } else {
              umountDriverError.put(volumeName,
                  ScsiClientOperationExceptionThrift.VolumeUmountingExceptionThrift);
            }
          }

          logger
              .warn("when umountDriverForScsi, find the volume :{} in client :{} is in status :{}",
                  volumeId, scsiIp, scsiClient.getDescriptionType());
          continue;
        }
      }

      //update status to REMOVING
      scsiClientStore.updateScsiDriverStatusAndDescription(scsiIp, volumeId, snapshotId,
          DriverStatusThrift.REMOVING.name(), ScsiDriverDescription.BeginUnmounting.name(),
          TaskType.UmountDriver.name());

      umountDriverRequest.setVolumeId(entry.getKey());
      umountDriverRequest.setSnapshotId(entry.getValue());

      //save task
      TaskRequestInfo taskRequestInfo = new TaskRequestInfo();
      taskRequestInfo.setTaskId(RequestIdBuilder.get());
      taskRequestInfo.setTaskType(TaskType.UmountDriver.name());
      taskRequestInfo.setRequest(umountDriverRequest);
      taskRequestInfo.setTaskCreateTime(System.currentTimeMillis());
      try {
        taskStore.saveTask(taskRequestInfo);
        volumeToTaskInfo.put(volumeId, taskRequestInfo.getTaskId());
      } catch (IOException e) {
        logger.error("when launchDriverForScsi, save the launchDriverRequest :{} errror",
            umountDriverRequest);
      }
    }

    logger.warn("umountDriverForScsi, the task info :{}", volumeToTaskInfo);
    UmountScsiDriverResponseThrift response = new UmountScsiDriverResponseThrift(
        request.getRequestId(),
        umountDriverError);
    logger.warn("umountDriverForScsi response: {}", response);
    return response;
  }

  /**
   * get disk smart info disk name like "/dev/sda" NOTICE: get disk smart info may be cause disk
   * write and read vibrate.
   */
  @Override
  public GetDiskSmartInfoResponseThrift getDiskSmartInfo(GetDiskSmartInfoRequestThrift request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      EndPointNotFoundExceptionThrift,
      TooManyEndPointFoundExceptionThrift, NetworkErrorExceptionThrift, TException {
    logger.warn("getDiskSmartInfo, request is:{}", request);

    String serverNodeId = request.getServerId();
    String diskName = request.getDiskName();

    //find servernode
    ServerNode serverNode = serverNodeStore.listServerNodeById(serverNodeId);
    if (serverNode == null) {
      logger.error("cannot found server node by server id:{}", serverNodeId);
      throw new ServerNodeNotExistExceptionThrift();
    }
    //get server node all ips
    Set<String> netCardIpSet = new HashSet<>(
        NodeInfo.parseNetCardInfoToGetIps(serverNode.getNetworkCardInfo()));

    //find SystemDaemon services
    /* this time not have SystemDaemon
     EndPoint endPoint = null;
     Set<Instance> instanceSet = instanceStore
              .getAll(PyService.SYSTEMDAEMON.getServiceName(), InstanceStatus.OK);
      for (Instance instance : instanceSet) {
          Map<PortType, EndPoint> instanceEndPoints = instance.getEndPoints();
          for (EndPoint endPointTemp : instanceEndPoints.values()) {
              if (netCardIpSet.contains(endPointTemp.getHostName())) {
                  endPoint = endPointTemp;
                  break;
              }
          }
      }
      if (endPoint == null) {
          logger.error("cannot found OK systemDaemon service on server node:{}", serverNodeId);
          throw new InstanceNotExistsExceptionThrift();
      }

      //get disk smart info from SD
      GetDiskSmartInfoResponseThrift response = null;
      SystemDaemon.Iface clientSd = null;
      try {
          clientSd = systemDaemonClientFactory.build().getClient();
          response = clientSd.getDiskSmartInfo(request);
      } catch (EndPointNotFoundException e) {
          logger.error("Caught an exception", e);
          throw new EndPointNotFoundExceptionThrift().setDetail(e.getMessage());
      } catch (TooManyEndPointFoundException e) {
          logger.error("Caught an exception", e);
          throw new TooManyEndPointFoundExceptionThrift().setDetail(e.getMessage());
      } catch (GenericThriftClientFactoryException e) {
          logger.error("Caught an exception", e);
          throw new NetworkErrorExceptionThrift().setDetail(e.getMessage());
      } catch (TException e) {
          logger.error("Caught an exception", e);
          throw e;
      } catch (Exception e) {
          logger.error("Caught an exception", e);
          throw new TException(e);
      }
    */

    GetDiskSmartInfoResponseThrift response = new GetDiskSmartInfoResponseThrift();
    logger.warn("getDiskSmartInfo, response is {}", response);
    return response;
  }

  @Override
  public ListScsiClientResponse listScsiClient(ListScsiClientRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      EndPointNotFoundExceptionThrift,
      TooManyEndPointFoundExceptionThrift, NetworkErrorExceptionThrift, TException {
    if (shutDownFlag) {
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (InstanceStatus.SUSPEND == appContext.getStatus()) {
      logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
      throw new ServiceIsNotAvailableThrift().setDetail("I am suspend");
    }

    logger.warn("listScsiClient request: {}", request);

    Long accountId = request.getAccountId();
    final Set<Long> accessibleResource = securityManager
        .getAccessibleResourcesByType(accountId, PyResource.ResourceType.Volume);

    String driverIp = request.getIp();
    Set<String> allDriverIps = new HashSet<>();
    List<ScsiClientDescriptionThrift> allDriverInfo = new ArrayList<>();
    List<ScsiClient> scsiClientInfo = null;

    ListScsiClientResponse response = new ListScsiClientResponse();
    //list all ip
    List<ScsiClient> scsiClientList = scsiClientStore.listAllScsiClients();
    if (scsiClientList != null && !scsiClientList.isEmpty()) {
      for (ScsiClient value : scsiClientList) {
        allDriverIps.add(value.getScsiClientInfo().getIpName());
      }
    }

    if (driverIp == null) {
      //set the all ip and status
      Set<Instance> driverContainerInstances = instanceStore
          .getAll(PyService.DRIVERCONTAINER.getServiceName(), InstanceStatus.HEALTHY);
      for (String clientIp : allDriverIps) {
        boolean getStatus = false;
        for (Instance instance : driverContainerInstances) {
          if (instance.getEndPoint().getHostName().equals(clientIp)) {
            getStatus = true;
            break;
          }
        }

        ScsiClientDescriptionThrift scsiClientDescriptionThrift = new ScsiClientDescriptionThrift();
        scsiClientDescriptionThrift.setIp(clientIp);
        if (getStatus) {
          scsiClientDescriptionThrift.setStatus(ScsiClientStatusThrift.OK);
        } else {
          scsiClientDescriptionThrift.setStatus(ScsiClientStatusThrift.ERROR);
        }

        allDriverInfo.add(scsiClientDescriptionThrift);
      }

      response.setClientDescriptions(allDriverInfo);
      logger.warn("listScsiClient response: {}", response);
      return response;

    } else {

      //get one ip info
      scsiClientInfo = scsiClientStore.getScsiClientByIp(driverIp);

      if (scsiClientInfo == null) {
        logger.error("listScsiClient, the input ip is error");
        throw new TException();
      }

      //get ip value
      Map<Long, Integer> volumeInfo = new HashMap<>();
      Set<Long> driverContainerIdWithPyd = new HashSet<>();
      long driverContainerIdScsi = 0;
      for (ScsiClient value : scsiClientInfo) {
        long volumeId = value.getScsiClientInfo().getVolumeId();
        int snapshotId = value.getScsiClientInfo().getSnapshotId();
        long driverContainerIdPyd = value.getDriverContainerId();

        if (volumeId != 0) {
          volumeInfo.put(volumeId, snapshotId);
        } else {
          //get the driverContainerIdScsi
          driverContainerIdScsi = value.getDriverContainerIdScsi();
        }

        if (driverContainerIdPyd != 0) {
          driverContainerIdWithPyd.add(driverContainerIdPyd);
        }
      }

      try {
        //by driverIp get drivercontainerId
        ListScsiDriverMetadataRequest listScsiDriverMetadataRequest =
            new ListScsiDriverMetadataRequest();
        listScsiDriverMetadataRequest.setRequestId(RequestIdBuilder.get());
        listScsiDriverMetadataRequest.setDriverIp(driverIp);
        listScsiDriverMetadataRequest.setDriverContainerIdScsi(driverContainerIdScsi);
        listScsiDriverMetadataRequest.setVolumeInfo(volumeInfo);
        listScsiDriverMetadataRequest.setDriverContainersIdWithPyd(driverContainerIdWithPyd);

        ListScsiDriverMetadataResponse listScsiDriverMetadataResponse = listScsiDriverMetadata(
            listScsiDriverMetadataRequest);
        logger.info("get the listScsiDriverMetadataResponse :{} ", listScsiDriverMetadataResponse);

        final List<VolumeMetadataThrift> UnUsedVolumeInfo = listScsiDriverMetadataResponse
            .getUnUsedVolumeInfos();
        List<ScsiClientInfoThrift> launchVolumeInfo = listScsiDriverMetadataResponse
            .getScsiClientInfo();
        List<DriverMetadataThrift> driverMetadataInfo = listScsiDriverMetadataResponse
            .getDriverInfo();

        //set the Description for launch Volume in scsi client
        for (ScsiClientInfoThrift scsiClientInfoThriftLaunch : launchVolumeInfo) {
          long volumeId = scsiClientInfoThriftLaunch.getVolume().getVolumeId();

          //scis report status
          ScsiDeviceStatusThrift scsiStatus = scsiClientInfoThriftLaunch.getStatus();
          logger.info("get the ScsiDeviceStatusThrift status :{}", scsiStatus);

          ScsiClient scsiClientDb = scsiClientStore
              .getScsiClientInfoByVolumeIdAndSnapshotId(driverIp, volumeId, 0);
          if (scsiClientDb != null) {

            final String descriptionDb = scsiClientDb.getStatusDescription();
            final String descriptionType = scsiClientDb.getDescriptionType();
            long driverContainerId = scsiClientDb.getDriverContainerId();

            DriverStatusThrift pydStatus = null;

            //get the pydStatus
            for (DriverMetadataThrift driverMetadata : driverMetadataInfo) {
              if (driverMetadata.getDriverContainerId() == driverContainerId
                  && driverMetadata.getVolumeId() == volumeId && driverMetadata.getSnapshotId() == 0
                  && DriverTypeThrift.NBD.equals(driverMetadata.getDriverType())) {
                pydStatus = driverMetadata.getDriverStatus();
              }
            }

            //status in db,just for launching and removing
            DriverStatusThrift scsiDriverStatusDb = DriverStatusThrift
                .valueOf(scsiClientDb.getScsiDriverStatus());

            //merge status
            DriverStatusThrift endStatus = mergePydDriverStatus(pydStatus, scsiStatus,
                scsiDriverStatusDb,
                volumeId);

            //for LAUNCHING
            if (DriverStatusThrift.LAUNCHING.equals(endStatus)) {
              scsiStatus = ScsiDeviceStatusThrift.CONNECTING;
            }

            //for REMOVING
            if (DriverStatusThrift.REMOVING.equals(endStatus)) {
              scsiStatus = ScsiDeviceStatusThrift.DISCONNECTING;
            }

            if (scsiStatus == null) {
              logger.warn("listScsiClient, can not get the scsiStatus");
              scsiStatus = ScsiDeviceStatusThrift.ERROR;
            }

            //set status
            scsiClientInfoThriftLaunch.setDriverStatus(endStatus);
            scsiClientInfoThriftLaunch.setStatus(scsiStatus);

            //set the description
            scsiClientInfoThriftLaunch.setDescriptionTpye(
                RequestResponseHelper.convertScsiDescriptionTpyeThrift(descriptionType));
            scsiClientInfoThriftLaunch.setStatusDescription(descriptionDb);
          }
        }

        //remove the volumes in other scsi client
        Map<Long, Integer> allVolumesInfoInClient = new HashMap<>();
        allDriverIps.remove(driverIp);
        for (String ip : allDriverIps) {
          List<ScsiClient> scsiClientInfoGet = scsiClientStore.getScsiClientByIp(ip);
          if (scsiClientInfoGet != null) {
            for (ScsiClient value : scsiClientInfoGet) {
              long volumeId = value.getScsiClientInfo().getVolumeId();
              int snapshotId = value.getScsiClientInfo().getSnapshotId();
              if (volumeId != 0) {
                allVolumesInfoInClient.put(volumeId, snapshotId);
              }
            }
          }
        }

        Set<Long> volumeInClient = allVolumesInfoInClient.keySet();
        logger.info("the volumes :{} which in other scsi client", volumeInClient);
        Iterator iterator = UnUsedVolumeInfo.iterator();
        while (iterator.hasNext()) {
          VolumeMetadataThrift volumeMetadataThrift = (VolumeMetadataThrift) iterator.next();
          if (volumeInClient.contains(volumeMetadataThrift.getVolumeId())) {
            iterator.remove();
          }
        }

        response.setLaunchVolumesForScsi(launchVolumeInfo);
        response.setUnLaunchVolumesForScsi(UnUsedVolumeInfo);

        logger.warn(
            "get the UsedVolumeInfo name :{}, volume status :{}, pyd status :{}, scsi status :{},"
                + " and path :{}, and Description :{},"
                + "get the UnUsedVolumeInfo name :{}, and status :{}", launchVolumeInfo.stream()
                .map(scsiClientInfoThrift -> scsiClientInfoThrift.getVolume().getName())
                .collect(Collectors.toList()), launchVolumeInfo.stream()
                .map(scsiClientInfoThrift -> scsiClientInfoThrift.getVolume().getVolumeStatus())
                .collect(Collectors.toList()),
            launchVolumeInfo.stream()
                .map(scsiClientInfoThrift -> scsiClientInfoThrift.getDriverStatus())
                .collect(Collectors.toList()),
            launchVolumeInfo.stream()
                .map(scsiClientInfoThrift -> scsiClientInfoThrift.getStatus())
                .collect(Collectors.toList()),
            launchVolumeInfo.stream().map(scsiClientInfoThrift -> scsiClientInfoThrift.getPath())
                .collect(Collectors.toList()), launchVolumeInfo.stream()
                .map(scsiClientInfoThrift -> scsiClientInfoThrift.getStatusDescription())
                .collect(Collectors.toList()),
            UnUsedVolumeInfo.stream().map(volumeMetadataThrift -> volumeMetadataThrift.getName())
                .collect(Collectors.toList()),
            UnUsedVolumeInfo.stream()
                .map(volumeMetadataThrift -> volumeMetadataThrift.getVolumeStatus())
                .collect(Collectors.toList()));

      } catch (TException e) {
        logger.error("listScsiClient find some error:", e);
        throw e;
      }
    }

    response.setClientDescriptions(allDriverInfo);
    response.setCurrentIp(driverIp);
    logger.info("listScsiClient response: {}", response);
    return response;
  }

  //check the status
  public ScsiDeviceStatusThrift mergeScsiDriverStatus(DriverStatusThrift pydStatus,
      ScsiDeviceStatusThrift scsiStatus, ScsiDeviceStatusThrift statusInDb, long volumeId) {
    logger.warn("the volume :{}, the pydStatus :{}, scsiStatus:{}, statusInDb:{}", volumeId,
        pydStatus, scsiStatus,
        statusInDb);
    ScsiDeviceStatusThrift scsiDeviceStatusThrift = null;

    //get status for pyd and db
    if (scsiStatus == null) {
      scsiDeviceStatusThrift = statusInDb;
      return scsiDeviceStatusThrift;
    }

    //if removing
    if (ScsiDeviceStatusThrift.REMOVING.equals(statusInDb)) {
      scsiDeviceStatusThrift = ScsiDeviceStatusThrift.REMOVING;
      return scsiDeviceStatusThrift;
    }

    //if pyd ok
    if (DriverStatusThrift.LAUNCHED.equals(pydStatus)) {
      scsiDeviceStatusThrift = scsiStatus;
      return scsiDeviceStatusThrift;
    }

    //if pyd no tok
    if (!DriverStatusThrift.LAUNCHED.equals(pydStatus)) {

      if (scsiStatus.equals(ScsiDeviceStatusThrift.NORMAL)) {
        scsiDeviceStatusThrift = ScsiDeviceStatusThrift.ERROR;
      } else {
        scsiDeviceStatusThrift = scsiStatus;
      }
      return scsiDeviceStatusThrift;
    }

    scsiDeviceStatusThrift = ScsiDeviceStatusThrift.UNKNOWN;
    logger.error("mergeScsiDriverStatus for volume :{}, find error", volumeId);
    return scsiDeviceStatusThrift;
  }

  public DriverStatusThrift mergePydDriverStatus(DriverStatusThrift pydStatus,
        ScsiDeviceStatusThrift scsiStatus,
        DriverStatusThrift statusInDb, long volumeId) {
    logger.warn("the volume :{}, the pydStatus :{}, scsiStatus:{}, statusInDb:{}", volumeId,
        pydStatus, scsiStatus,
        statusInDb);
    DriverStatusThrift driverStatusThrift = null;

    //get status for pyd and db
    if (scsiStatus == null) {
      driverStatusThrift = statusInDb;
      return driverStatusThrift;
    }

    //if removing
    if (DriverStatusThrift.REMOVING.equals(statusInDb)) {
      driverStatusThrift = DriverStatusThrift.REMOVING;
      return driverStatusThrift;
    }

    //if pyd ok
    if (pydStatus != null) {
      driverStatusThrift = pydStatus;
      return driverStatusThrift;
    }

    //set default
    driverStatusThrift = DriverStatusThrift.UNKNOWN;
    logger.warn("mergeScsiDriverStatus for volume :{}, find error", volumeId);
    return driverStatusThrift;
  }

  public ListScsiDriverMetadataResponse listScsiDriverMetadata(
      ListScsiDriverMetadataRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift, TException {
    if (shutDownFlag) {
      throw new ServiceHavingBeenShutdownThrift();
    }

    if (InstanceStatus.SUSPEND == appContext.getStatus()) {
      logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
      throw new ServiceIsNotAvailableThrift().setDetail("I am suspend");
    }
    logger.warn("listScsiDriverMetadata request: {}", request);

    final String driverIp = request.getDriverIp();
    final Map<Long, Integer> volumeInfo = request.getVolumeInfo();
    List<ScsiClientInfoThrift> scsiClientInfoFormInfocenterList = new ArrayList<>();
    List<VolumeMetadataThrift> volumeMetadataThriftList = new ArrayList<>();

    //list all volume
    ListVolumesRequest listVolumesRequest = new ListVolumesRequest();
    listVolumesRequest.setRequestId(RequestIdBuilder.get());
    listVolumesRequest.setContainDeadVolume(false);
    ListVolumesResponse listVolumesResponse = listVolumes(listVolumesRequest);
    volumeMetadataThriftList = listVolumesResponse.getVolumes();

    //get the set volume
    List<ScsiDriverMetadata> scsiDriverMetadataList = new ArrayList<>();
    long drivercontainerIdScsi = request.getDriverContainerIdScsi();
    if (drivercontainerIdScsi != 0) {
      scsiDriverMetadataList = scsiDriverStore.getByDriverContainerId(drivercontainerIdScsi);
    }

    for (Map.Entry<Long, Integer> entry : volumeInfo.entrySet()) {
      long volumeId = entry.getKey();
      int snapshotId = entry.getValue();
      VolumeMetadataThrift volumeMetadataThrift = null;

      //get the unlauch volume info, and remove current volume in unlauch volumes
      Iterator iterator = volumeMetadataThriftList.iterator();
      while (iterator.hasNext()) {
        VolumeMetadataThrift volumeMetadataThriftGet = (VolumeMetadataThrift) iterator.next();
        //remove it in lauch and only need Stable and Available volume
        if (volumeId == volumeMetadataThriftGet.getVolumeId()) {
          volumeMetadataThrift = volumeMetadataThriftGet;
          iterator.remove();
        }
      }

      if (volumeMetadataThrift == null) {
        logger.error("when listScsiDriverMetadata, can not find the volume :{}", volumeId);
        continue;
      }

      boolean findvoluminscsidriverstore = false;
      ScsiClientInfoThrift scsiClientInfoThrift = new ScsiClientInfoThrift();
      for (ScsiDriverMetadata scsiDriverMetadata : scsiDriverMetadataList) {

        DriverKeyForScsi driverKeyForScsi = scsiDriverMetadata.getDriverKeyForScsi();
        logger.info("get the driverKeyForScsi value :{}", driverKeyForScsi.toString());

        if (driverKeyForScsi != null && volumeId == driverKeyForScsi.getVolumeId()) {
          scsiClientInfoThrift.setVolume(volumeMetadataThrift);
          scsiClientInfoThrift
              .setStatus(buildScsiDeviceStatusType(scsiDriverMetadata.getScsiDeviceStatus()));
          scsiClientInfoThrift.setPath(scsiDriverMetadata.getScsiDevice());
          scsiClientInfoFormInfocenterList.add(scsiClientInfoThrift);
          findvoluminscsidriverstore = true;
          break;
        }
      }

      //not find the volume with scsi info, set the status LAUNCHING
      if (!findvoluminscsidriverstore) {
        scsiClientInfoThrift.setVolume(volumeMetadataThrift);
        //                scsiClientInfoThrift.setStatus(ScsiDeviceStatusThrift.LAUNCHING);
        scsiClientInfoFormInfocenterList.add(scsiClientInfoThrift);
        logger.warn("when listScsiDriverMetadata, can not get the volume :{} scsi info", volumeId);
      }
    }

    //list Driver
    List<DriverMetadataThrift> driverMetadataThriftList = new ArrayList<>();
    List<DriverMetadata> driverMetadatas = new ArrayList();
    Set<Long> driverContainersIdWithPyd = request.getDriverContainersIdWithPyd();
    for (Long value : driverContainersIdWithPyd) {
      driverMetadatas.addAll(driverStore.getByDriverContainerId(value));
    }

    // to thirf
    for (DriverMetadata driverMetadata : driverMetadatas) {
      driverMetadataThriftList
          .add(RequestResponseHelper.buildThriftDriverMetadataFrom(driverMetadata));
    }

    ListScsiDriverMetadataResponse response = new ListScsiDriverMetadataResponse();
    response.setRequestId(request.getRequestId());
    response.setDriverIp(driverIp);
    response.setScsiClientInfo(scsiClientInfoFormInfocenterList);
    response.setUnUsedVolumeInfos(volumeMetadataThriftList);
    response.setDriverInfo(driverMetadataThriftList);

    logger.warn(
        "listScsiDriverMetadata result, get the UsedVolumeInfo name :{}, and status :{}, and path"
            + " :{}"
            + "get the UnUsedVolumeInfo name :{}, and status :{}",
        scsiClientInfoFormInfocenterList.stream()
            .map(scsiClientInfoThrift -> scsiClientInfoThrift.getVolume().getName())
            .collect(Collectors.toList()), scsiClientInfoFormInfocenterList.stream()
            .map(scsiClientInfoThrift -> scsiClientInfoThrift.getStatus())
            .collect(Collectors.toList()),
        scsiClientInfoFormInfocenterList.stream()
            .map(scsiClientInfoThrift -> scsiClientInfoThrift.getPath())
            .collect(Collectors.toList()),
        volumeMetadataThriftList.stream()
            .map(volumeMetadataThrift -> volumeMetadataThrift.getName())
            .collect(Collectors.toList()),
        volumeMetadataThriftList.stream()
            .map(volumeMetadataThrift -> volumeMetadataThrift.getVolumeStatus())
            .collect(Collectors.toList()));
    logger.info("listScsiDriverMetadata response: {}", response);

    return response;
  }

  @Override
  public CreateScsiClientResponse createScsiClient(CreateScsiClientRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      ScsiClientIsExistExceptionThrift,
      AccountNotFoundExceptionThrift, PermissionNotGrantExceptionThrift, TException {
    if (shutDownFlag) {
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (InstanceStatus.SUSPEND == appContext.getStatus()) {
      logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
      throw new ServiceIsNotAvailableThrift().setDetail("I am suspend");
    }

    logger.warn("createScsiClient request: {}", request);

    securityManager.hasPermission(request.getAccountId(), "createScsiClient");

    String ipName = request.getIp();
    List<ScsiClient> scsiClientList = scsiClientStore.getScsiClientByIp(ipName);

    //have
    if (scsiClientList != null && !scsiClientList.isEmpty()) {
      throw new ScsiClientIsExistExceptionThrift();
    }

    ScsiClient scsiClient = new ScsiClient();
    ScsiClientInfo scsiClientInfo = new ScsiClientInfo();
    scsiClientInfo.setIpName(ipName);

    scsiClient.setScsiClientInfo(scsiClientInfo);
    scsiClient
        .setDriverContainerIdScsi(request.getDriverContainerId()); //the scsi DriverContainerId

    //save
    scsiClientStore.saveOrUpdateScsiClientInfo(scsiClient);
    buildEndOperationWrtCreateScsiClientAndSaveToDb(request.getAccountId(), ipName);

    CreateScsiClientResponse response = new CreateScsiClientResponse();
    logger.warn("createScsiClient response: {}", response);
    return response;
  }

  @Override
  public DeleteScsiClientResponse deleteScsiClient(DeleteScsiClientRequest request)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift,
      AccountNotFoundExceptionThrift,
      PermissionNotGrantExceptionThrift, TException {
    if (shutDownFlag) {
      throw new ServiceHavingBeenShutdownThrift();
    }

    if (InstanceStatus.SUSPEND == appContext.getStatus()) {
      logger.error("Refuse request from remote due to I am suspend, request content: {}", request);
      throw new ServiceIsNotAvailableThrift().setDetail("I am suspend");
    }

    logger.warn("deleteScsiClient request: {}", request);
    securityManager.hasPermission(request.getAccountId(), "deleteScsiClient");

    Map<String, ScsiClientOperationExceptionThrift> error = new HashMap<>();
    List<String> ips = request.getIps();

    if (ips == null && ips.isEmpty()) {
      logger.error("when deleteScsiClient, the input value is error");
      throw new TException();
    }

    StringBuilder operationInfo = new StringBuilder();

    for (String ip : ips) {
      List<ScsiClient> scsiClientList = scsiClientStore.getScsiClientByIp(ip);
      if (scsiClientList != null && !scsiClientList.isEmpty()) {
        boolean someVolumeInClient = false;
        for (ScsiClient scsiClient : scsiClientList) {
          if (scsiClient.getScsiClientInfo().getVolumeId() != 0) {
            //not default
            someVolumeInClient = true;
            break;
          }
        }

        if (someVolumeInClient) {
          error.put(ip,
              ScsiClientOperationExceptionThrift.ScsiClientHaveLaunchedVolumeExceptionThrift);
          logger.warn("when delete client :{}, find some volume in client", ip);
          buildEndOperationWrtDeleteErrorScsiClientAndSaveToDb(request.getAccountId(), ip);
          continue;
        }
      }

      scsiClientStore.deleteScsiClientByIp(ip);
      operationInfo.append(ip).append(",");

    }

    //set
    String operationInfoString = operationInfo.toString();
    if (!operationInfoString.isEmpty()) {
      int length = operationInfoString.length();
      buildEndOperationWrtDeleteScsiClientAndSaveToDb(request.getAccountId(),
          operationInfo.toString().substring(0, length - 1));
    }

    DeleteScsiClientResponse response = new DeleteScsiClientResponse();
    response.setError(error);
    logger.warn("deleteScsiClient response: {}", response);
    return response;
  }

  /**
   * get OK instance disk info.
   */
  public Map<String, Map<String, DiskInfo>> getInstanceDiskInfo() {
    Set<String> instanceIpSet = new HashSet<>();
    for (InstanceMetadata instance : storageStore.list()) {
      if (instance.getDatanodeStatus().equals(OK)) {
        instanceIpSet.add(instance.getEndpoint());
      }
    }

    Map<String, Map<String, DiskInfo>> instanceIp2DiskInfoMap = new HashMap<>();
    if (instanceIpSet.isEmpty()) {
      logger.warn("found 0 OK instance from storage store");
      return instanceIp2DiskInfoMap;
    }

    logger.debug("get instanceIpSet: {}", instanceIpSet);

    //get archive info

    List<ServerNode> serverNodeList = serverNodeStore
        .listServerNodes(0, 100000, null, null, null, null, null, null, null, null, null, null,
            null, null,
            null);
    logger.debug("get serverNodeList: {} {}", serverNodeList.size(), serverNodeList);
    for (ServerNode serverNode : serverNodeList) {
      for (String instanceIp : instanceIpSet) {
        String ip = instanceIp.substring(0, instanceIp.indexOf(":"));
        if (serverNode.getNetworkCardInfo().contains(ip)) {
          Map<String, DiskInfo> diskName2DiskInfo = new HashMap<>();
          for (DiskInfo diskInfo : serverNode.getDiskInfoSet()) {
            diskName2DiskInfo.put(diskInfo.getName(), diskInfo);
          }
          instanceIp2DiskInfoMap.put(instanceIp, diskName2DiskInfo);
          break;
        }   //if (serverNode.getNetworkCardInfo().contains(instanceIp)) {
      }   //for (String instanceIp : instanceIpSet) {
    }   //for (ServerNode serverNode : serverNodeList) {
    logger.info("get disk info: {}", instanceIp2DiskInfoMap);
    return instanceIp2DiskInfoMap;
  }

  
  private long ceilVolumeSizeWithSegmentSize(long expectVolumeSize)
      throws VolumeSizeIllegalExceptionThrift {
    if (expectVolumeSize <= 0) {
      logger.error("volume size:{} illegal. must be > 0", expectVolumeSize);
      throw new VolumeSizeIllegalExceptionThrift();
    }

    long factSegmentCount = expectVolumeSize / segmentSize;
    long remainder = expectVolumeSize % segmentSize;

    if (remainder > 0) {
      factSegmentCount++;
    }

    return (factSegmentCount * segmentSize);
  }

  /**
   * filter xxxThrift.
   */
  private TException filterTexception(TException e) {
    if (e instanceof InternalErrorThrift) {
      e = new TException(e);
    }

    return e;
  }


  public List<DriverMetadata> getDriversByVolumeId(List<DriverMetadata> drivers, long volumeId) {
    List<DriverMetadata> driverMetadatas = new ArrayList<>();

    for (DriverMetadata driverMetadata : drivers) {
      if (driverMetadata.getVolumeId() == volumeId) {
        driverMetadatas.add(driverMetadata);
      }
    }
    return driverMetadatas;
  }


  
  public List<DriverMetadata> getDriversBySnapshotId(List<DriverMetadata> drivers, int snapshotId) {
    List<DriverMetadata> driverMetadatas = new ArrayList<>();

    for (DriverMetadata driverMetadata : drivers) {
      if (driverMetadata.getSnapshotId() == snapshotId) {
        driverMetadatas.add(driverMetadata);
      }
    }
    return driverMetadatas;
  }


  
  public List<DriverMetadata> getDriversByDcHost(List<DriverMetadata> drivers,
      String drivercontainerHost) {
    List<DriverMetadata> driverMetadatas = new ArrayList<>();

    for (DriverMetadata driverMetadata : drivers) {
      if (driverMetadata.getQueryServerIp().equals(drivercontainerHost)) {
        driverMetadatas.add(driverMetadata);
      }
    }
    return driverMetadatas;
  }


  
  public List<DriverMetadata> getDriversByDriverHost(List<DriverMetadata> drivers,
      String driverHost) {
    List<DriverMetadata> driverMetadatas = new ArrayList<>();

    for (DriverMetadata driverMetadata : drivers) {
      if (driverMetadata.getHostName().equals(driverHost)) {
        driverMetadatas.add(driverMetadata);
      }
    }
    return driverMetadatas;
  }


  
  public List<DriverMetadata> getDriversByDriverType(List<DriverMetadata> drivers,
      DriverType driverType) {
    List<DriverMetadata> driverMetadatas = new ArrayList<>();

    for (DriverMetadata driverMetadata : drivers) {
      if (driverMetadata.getDriverType().equals(driverType)) {
        driverMetadatas.add(driverMetadata);
      }
    }

    return driverMetadatas;
  }

  private List<DriverClientInformation> getDriverClientInfo(
      List<DriverClientInformation> driverClientInformationList, String driverName,
      String volumeName, String volumeDescription) {
    List<DriverClientInformation> driverClientInformations = new ArrayList<>();
    if (driverClientInformationList == null || driverClientInformationList.isEmpty()) {
      return driverClientInformations;
    }

    String driverNameToLowerCase = null;
    String volumeNameToLowerCase = null;
    String volumeDescriptionToLowerCase = null;

    if (driverName != null && !driverName.trim().isEmpty()) {
      driverNameToLowerCase = driverName.toLowerCase();
    }

    if (volumeName != null && !volumeName.trim().isEmpty()) {
      volumeNameToLowerCase = volumeName.toLowerCase();
    }

    if (volumeDescription != null && !volumeDescription.trim().isEmpty()) {
      volumeDescriptionToLowerCase = volumeDescription.toLowerCase();
    }

    for (DriverClientInformation driverClientInformation : driverClientInformationList) {
      String driverNameTemp = driverClientInformation.getDriverName();
      String volumeNameTemp = driverClientInformation.getVolumeName();
      String volumeDescriptionTemp = driverClientInformation.getVolumeDescription();

      //by driverName
      if (driverNameToLowerCase != null) {
        if (driverNameTemp == null
            || !driverNameTemp.toLowerCase().contains(driverNameToLowerCase)) {
          continue;
        }
      }

      if (volumeNameToLowerCase != null) {
        if (volumeNameTemp == null
            || !volumeNameTemp.toLowerCase().contains(volumeNameToLowerCase)) {
          continue;
        }
      }

      if (volumeDescriptionToLowerCase != null) {
        if (volumeDescriptionTemp == null
            || !volumeDescriptionTemp.toLowerCase().contains(volumeDescriptionToLowerCase)) {
          continue;
        }
      }

      //get
      driverClientInformations.add(driverClientInformation);
    }
    return driverClientInformations;
  }

  /**
   * get rebalance tasks of the migrate source as instance.
   *
   * @param instanceId migrate source instance
   * @return rebalance task list
   */
  public RebalanceTaskListThrift getRebalanceTasks(InstanceId instanceId) {
    RebalanceTaskListThrift rebalanceTaskListThrift = new RebalanceTaskListThrift();
    try {
      Multimap<Long, SendRebalanceTask> rebalanceTaskList = segmentUnitsDistributionManager
          .selectRebalanceTasks(instanceId);
      for (Entry<Long, SendRebalanceTask> entry : rebalanceTaskList.entries()) {
        long volumeId = entry.getKey();
        SendRebalanceTask rebalanceTask = entry.getValue();
        SimulateSegmentUnit sourceSegmentUnit = rebalanceTask.getSourceSegmentUnit();
        SegId segId = sourceSegmentUnit.getSegId();
        SegIdThrift segIdThrift = RequestResponseHelper.buildThriftSegIdFrom(segId);
        long srcId = rebalanceTask.getInstanceToMigrateFrom().getId();
        long destId = rebalanceTask.getDestInstanceId().getId();

        if (rebalanceTask.getTaskType() == BaseRebalanceTask.RebalanceTaskType.PrimaryRebalance) {
          rebalanceTaskListThrift
              .addToPrimaryMigrateList(new PrimaryMigrateThrift(segIdThrift, srcId, destId));
        } else if (rebalanceTask.getTaskType() == BaseRebalanceTask.RebalanceTaskType.PSRebalance) {
          VolumeMetadata volumeMetadata = null;
          try {

            //check volume status
            volumeMetadata = volumeInformationManger
                .getVolume(volumeId, Constants.SUPERADMIN_ACCOUNT_ID);
            if (VolumeStatus.Available.equals(volumeMetadata.getVolumeStatus())
                || VolumeStatus.Stable
                .equals(volumeMetadata.getVolumeStatus())) {
              volumeMetadata = volumeInformationManger
                  .getVolumeNew(volumeId, Constants.SUPERADMIN_ACCOUNT_ID);
            } else {
              logger.warn("when getRebalanceTasks, the volume :{} {} status :{}, no need check",
                  volumeId,
                  volumeMetadata.getName(), volumeMetadata.getVolumeStatus());
              continue;
            }

          } catch (VolumeNotFoundException e) {
            logger.warn("when getRebalanceTasks, can not find the volume :{}", volumeId);
            continue;
          }

          SecondaryMigrateThrift secondaryMigrateThrift = new SecondaryMigrateThrift(segIdThrift,
              srcId,
              destId, sourceSegmentUnit.getVolumeType().getVolumeTypeThrift(),
              null,
              volumeMetadata.getStoragePoolId(),
              sourceSegmentUnit.getSegmentUnitType().getSegmentUnitTypeThrift(),
              volumeMetadata.getSegmentWrappCount(),
              sourceSegmentUnit.isEnableLaunchMultiDrivers(),
              sourceSegmentUnit.getVolumeSource().getVolumeSourceThrift());

          secondaryMigrateThrift.setInitMembership(RequestResponseHelper
              .buildThriftMembershipFrom(sourceSegmentUnit.getSegId(),
                  volumeMetadata.getSegmentByIndex(segId.getIndex()).getLatestMembership()));

          rebalanceTaskListThrift.addToSecondaryMigrateList(secondaryMigrateThrift);
        } else if (rebalanceTask.getTaskType()
            == BaseRebalanceTask.RebalanceTaskType.ArbiterRebalance) {
          VolumeMetadata volumeMetadata = null;
          try {
            volumeMetadata = volumeInformationManger
                .getVolumeNew(volumeId, Constants.SUPERADMIN_ACCOUNT_ID);
          } catch (VolumeNotFoundException e) {
            logger.warn("when getRebalanceTasks, can not find the volume :{}", volumeId);
            continue;
          }

          ArbiterMigrateThrift arbiterMigrateThrift = new ArbiterMigrateThrift(segIdThrift,
              srcId, destId,
              sourceSegmentUnit.getVolumeType().getVolumeTypeThrift(),
              null,
              volumeMetadata.getStoragePoolId(),
              sourceSegmentUnit.getSegmentUnitType().getSegmentUnitTypeThrift(),
              volumeMetadata.getSegmentWrappCount(),
              sourceSegmentUnit.isEnableLaunchMultiDrivers(),
              sourceSegmentUnit.getVolumeSource().getVolumeSourceThrift());

          arbiterMigrateThrift.setInitMembership(RequestResponseHelper
              .buildThriftMembershipFrom(sourceSegmentUnit.getSegId(),
                  volumeMetadata.getSegmentByIndex(segId.getIndex()).getLatestMembership()));

          rebalanceTaskListThrift.addToArbiterMigrateList(arbiterMigrateThrift);
        }
      }
    } catch (NoNeedToRebalance e) {
      logger.debug("instance:{} has no task of rebalance", instanceId);
    }

    return rebalanceTaskListThrift;
  }


  
  public void checkExceptionForOperation(VolumeMetadata volumeMetadata,
      OperationFunctionType operationFunctionType)
      throws VolumeIsCopingExceptionThrift, VolumeIsCloningExceptionThrift,
      VolumeIsBeginMovedExceptionThrift,
      VolumeInMoveOnlineDoNotHaveOperationExceptionThrift, VolumeInExtendingExceptionThrift,
      VolumeIsMovingExceptionThrift, VolumeCyclingExceptionThrift, VolumeDeletingExceptionThrift,
      VolumeNotAvailableExceptionThrift {

    ExceptionType exceptionType = volumeMetadata.getInAction()
        .checkOperation(operationFunctionType);
    if (exceptionType == null) {
      return;
    }
    exceptionForOperation
        .checkException(exceptionType, operationFunctionType, volumeMetadata.getVolumeId());
  }

  /**
   * get driver container if request volume is not in state launching.
   */
  public List<DriverContainerCandidate> allocDriverContainer(long volumeId, int driverAmount,
      boolean getForScsiDriver) throws TooManyDriversExceptionThrift {

    logger.warn("allocDriverContainer ,the volumeId: {} and driverAmount :{}", volumeId,
        driverAmount);
    //just for scsi Driver
    List<DriverContainerCandidate> driverContainerCandidateList = new ArrayList<>();

    // prepare response for request getting driver container
    synchronized (driverStore) {
     
     
      Set<Instance> allDriverContainers = instanceStore
          .getAll(PyService.DRIVERCONTAINER.getServiceName(), InstanceStatus.HEALTHY);
      List<DriverMetadata> volumeBindingDriverList = driverStore.get(volumeId);
      logger.warn("get all driver containers:{}, volumeBindingDriverList:{}", allDriverContainers,
          volumeBindingDriverList);
      Set<Instance> candidateDriverContainers = new HashSet<>();

      //scsi
      if (getForScsiDriver) {
        for (Instance instance : allDriverContainers) {
          //check for scsi
          if (instance.getDcType().equals(DcType.ALLSUPPORT) || instance.getDcType()
              .equals(DcType.NORMALSUPPORT)) {
            candidateDriverContainers.add(instance);
          }
        }
      } else {
        for (Instance instance : allDriverContainers) {
          candidateDriverContainers.add(instance);
        }
      }

      // available driver container is less than expected driver amount;
      if (candidateDriverContainers.size() < driverAmount) {
        logger.error(
            "Only {} driver containers is able to launch driver for volume {}, less than expected"
                + " {} driver containers",
            candidateDriverContainers.size(), volumeId, driverAmount);
        throw new TooManyDriversExceptionThrift();
      }

      // balance sort all candidates according to amount of launched
      // drivers
      List<DriverMetadata> allDriverMetadatas = driverStore.list();
      List<DriverContainerCandidate> allCandidatesSortedInBalance = driverContainerSelectionStrategy
          .select(candidateDriverContainers, allDriverMetadatas);

      for (DriverContainerCandidate oneCandidateOfAll : allCandidatesSortedInBalance) {
        driverContainerCandidateList.add(oneCandidateOfAll);
      }
    }

    logger.warn("response driver containers {}, size: {}", driverContainerCandidateList,
        driverContainerCandidateList.size());
    return driverContainerCandidateList;
  }


  
  public VolumeMetadata processVolumeForPagination(VolumeMetadata srcVolume, boolean addSegments,
      boolean enablePagination, int startSegmentIndex, int paginationNumber) {

    VolumeMetadata virtualVolume = new VolumeMetadata();
    int totalSegmentCount = srcVolume.getSegmentCount();
    int getRealStartIndex = 0;
    int getRealEndIndex = 0;
   
    int nextStartSegmentIndex = 0;

    if (addSegments) {
      if (enablePagination) {
        virtualVolume.deepCopyNotIncludeSegment(srcVolume);

        /*1. index large the totalSegmentCount **/
        if (startSegmentIndex >= totalSegmentCount) {
          getRealEndIndex = 0; //not have any Segment
        }

        /*2. index in (startSegmentIndex, totalSegmentCount) ***/
        if (startSegmentIndex < totalSegmentCount
            && startSegmentIndex + paginationNumber >= totalSegmentCount) {
          getRealStartIndex = startSegmentIndex;
          getRealEndIndex = totalSegmentCount;
        }

        /*3. index in (startSegmentIndex, startSegmentIndex + paginationNumber) ***/
        if (startSegmentIndex + paginationNumber <= totalSegmentCount) {
          getRealStartIndex = startSegmentIndex;
          getRealEndIndex = startSegmentIndex + paginationNumber;
        }

        logger.info(
            "when enablePagination, get the paginationNumber, getRealStartIndex, getRealEndIndex,"
                + " segmentMapsize,{} {} {} {}",
            paginationNumber, getRealStartIndex, getRealEndIndex, totalSegmentCount);

        //set nextStartSegmentIndex
        nextStartSegmentIndex = getRealEndIndex;

        //set Segment
        for (int i = getRealStartIndex; i < getRealEndIndex; i++) {
          SegmentMetadata segmentMetadata = srcVolume.getSegmentTable().get(i);
          if (segmentMetadata != null) {
            //may be the volume is not Available
            virtualVolume.getSegmentTable().put(i, srcVolume.getSegmentTable().get(i));
          }
        }

        //just for log
        List<Integer> indexList = new ArrayList<>();
        for (Integer key : virtualVolume.getSegmentTable().keySet()) {
          indexList.add(key);
        }

        if (!indexList.isEmpty()) {
          Collections.sort(indexList);
          logger.info("get the indexList start:{} to end :{} and size :{}", indexList.get(0),
              indexList.get(indexList.size() - 1), indexList.size());
        }

      } else {
        //not enablePagination
        virtualVolume.deepCopy(srcVolume);
        getRealEndIndex = startSegmentIndex + paginationNumber;
        nextStartSegmentIndex = totalSegmentCount;

      }
    } else {
      // not include Segments
      virtualVolume.deepCopyNotIncludeSegment(srcVolume);
      getRealEndIndex = startSegmentIndex + paginationNumber;
    }

    //return value
    if (totalSegmentCount > getRealEndIndex) {
      virtualVolume.setLeftSegment(true);
    } else {
      virtualVolume.setLeftSegment(false);
    }

    virtualVolume.setNextStartSegmentIndex(nextStartSegmentIndex);

    logger.info("get the return Segment size :{}, setLeftSegment :{}, NextStart :{}",
        virtualVolume.getSegmentTable().size(), virtualVolume.isLeftSegment(),
        virtualVolume.getNextStartSegmentIndex());

    return virtualVolume;
  }

  private void updateStoragePoolWithDomainName(Domain domainToBeUpdate) {
    Long domainId = domainToBeUpdate.getDomainId();
    List<StoragePool> storagePoolList = new ArrayList<>();

    try {
      storagePoolList = storagePoolStore.listStoragePools(domainId);
    } catch (SQLException e) {
      logger.error("In updateStoragePoolWithDomainName, we meet an SQLException.", e);
    } catch (IOException e) {
      logger.error("In updateStoragePoolWithDomainName, we meet an IOException.", e);
    }

    if (storagePoolList == null) {
      return;
    }

   
   
    String domainName = domainToBeUpdate.getDomainName();
    for (StoragePool storagePool : storagePoolList) {
      storagePool.setDomainName(domainName);
      storagePoolStore.saveStoragePool(storagePool);
    }
  }


  
  public void markDriverStatus(long volumeId, List<DriverIpTargetThrift> driverIpTargetList)
      throws VolumeNotFoundExceptionThrift {

    VolumeMetadata volumeMetadata = volumeStore.getVolume(volumeId);

    if (volumeMetadata == null) {
      logger.error("No volume found with id {}", volumeId);
      throw new VolumeNotFoundExceptionThrift();
    }

    if (driverIpTargetList == null || driverIpTargetList.isEmpty()) {
      return;
    }

    for (DriverIpTargetThrift driverTarget : driverIpTargetList) {
      DriverType driverType = DriverType.valueOf(driverTarget.getDriverType().name());
      DriverMetadata driver = driverStore
          .get(driverTarget.getDriverContainerId(), volumeId, driverType,
              driverTarget.getSnapshotId());
      if (driver == null) {
        logger.warn("can not get the driver: volumeId {}, hostname {}, snapshot ID {}", volumeId,
            driverTarget.getDriverIp(), driverTarget.getSnapshotId());
        continue;
      }

      driver.setDriverStatus(DriverStatus.REMOVING);
      driverStore.save(driver);
    }
  }

  private void mergePerformanceParameterMap(Map<Long, Long> to, Map<Long, Long> from) {
    if (from == null || from.size() == 0) {
      return;
    }

    TreeSet<Long> toKeySet = new TreeSet<>();
    for (Long key : to.keySet()) {
      toKeySet.add(key);
    }

    TreeSet<Long> fromKeySet = new TreeSet<>();
    for (Long key : from.keySet()) {
      fromKeySet.add(key);
    }
    int fromInterval = (int) (fromKeySet.higher(fromKeySet.first()) - fromKeySet.first());

    List<Long> newKeyFromFrom = new ArrayList<>();
    Long higherToKey;
    Long lowerToKey;
    Long toKey;
    Long newValue;
    Boolean higerMerge = null;
    for (Long fromKey : fromKeySet) {
      higherToKey = toKeySet.higher(fromKey);
      lowerToKey = toKeySet.lower(fromKey);
      if (higerMerge == null) {
        if (higherToKey == null) {
          newKeyFromFrom.add(fromKey);
        } else if (higherToKey - fromKey > fromInterval) {
          newKeyFromFrom.add(fromKey);
        } else if (higherToKey - fromKey > fromInterval / 2) {
          newKeyFromFrom.add(fromKey);
          higerMerge = new Boolean(false);
        } else {
          higerMerge = new Boolean(true);
        }
      }
      if (higerMerge != null) {
        if (higerMerge) {
          toKey = higherToKey;
        } else {
          toKey = lowerToKey;
        }
        if (toKey != null) {
          newValue = from.get(fromKey) + to.get(toKey);
          to.put(toKey, newValue);
        } else {
          newKeyFromFrom.add(fromKey);
        }
      }
    }
    for (Long newKey : newKeyFromFrom) {
      to.put(newKey, from.get(newKey));
    }

  }

  private Capacity getSystemCapacity(long accountId) {
    Capacity capacity = new Capacity();
    try {
      // get storage capacity data
      Capacity reutrnCapacity = getSystemCapacityFromInfocenter(accountId);
      if (reutrnCapacity != null) {
        capacity.setTotalCapacity(reutrnCapacity.getTotalCapacity());
        capacity.setAvailableCapacity(reutrnCapacity.getAvailableCapacity());
        capacity.setFreeSpace(reutrnCapacity.getFreeSpace());
      } else {
        logger.error("in getSystemCapacity caught an exception: reutrnCapacity is null.");
        return capacity;
      }

      DecimalFormat df = new DecimalFormat(".###");
      double freePer = ((double) Long.parseLong(capacity.getFreeSpace()) * 100) / Long
          .parseLong(capacity.getAvailableCapacity());
      double usedPer = 100 - freePer;

      final String freePerStr = df.format(freePer) + "%";
      final String usedPerStr = df.format(usedPer) + "%";

      double totalAvailableCapacity =
          ((double) Long.parseLong(capacity.getAvailableCapacity())) / gbSize;

      //the all unit space
      double freeSpace = ((double) Long.parseLong(capacity.getFreeSpace())) / gbSize;

      totalAvailableCapacity = Double.parseDouble(df.format(totalAvailableCapacity));
      freeSpace = Double.parseDouble(df.format(freeSpace));
      double usedSpace = Double.parseDouble(df.format(totalAvailableCapacity - freeSpace));

      capacity.setTotalCapacity(String.valueOf(totalAvailableCapacity));
      capacity.setFreeSpace(String.valueOf(freeSpace));
      capacity.setUsedCapacity(String.valueOf(usedSpace));

      capacity.setAvailableCapacityPer(freePerStr);
      capacity.setUsedCapacityPer(usedPerStr);

      /* get the all volume totalFreeSpace  ***/
      double totalUnitUsedSpace = getAllVolumeUnitUsedInfo();

      String theUsedUniPerStr;
      String theunUsedUniPerStr;
      double theUsedUnitSpace = 0.0;
      double theunUsedUnitSpace = 0.0;

      //the all un used
      if (totalUnitUsedSpace == 0.0) {
        theUsedUnitSpace = 0.0;
      }
      if ((usedSpace - (double) totalUnitUsedSpace) >= 0) {
        theunUsedUnitSpace = usedSpace - totalUnitUsedSpace;
        theUsedUnitSpace = totalUnitUsedSpace;
      } else {
        theUsedUnitSpace = usedSpace;
        theunUsedUnitSpace = 0.0;
      }

      double totalUnitSpace = theUsedUnitSpace + theunUsedUnitSpace;
      double usedUnitPer;
      double unusedUniPer;
      if (totalUnitSpace > 0) {
        usedUnitPer = theUsedUnitSpace * 100 / totalUnitSpace;
        unusedUniPer = 100 - usedUnitPer;
      } else {
        usedUnitPer = 0.0;
        unusedUniPer = 0.0;
      }

      if (usedUnitPer == 0.0) {
        theUsedUniPerStr = "0.0" + "%";
      } else {
        theUsedUniPerStr = df.format(usedUnitPer) + "%";
      }

      if (unusedUniPer == 0.0) {
        theunUsedUniPerStr = "0.0" + "%";
      } else {
        theunUsedUniPerStr = df.format(unusedUniPer) + "%";
      }

      capacity.setTheUsedUnitSpace(String.valueOf(theUsedUnitSpace));
      capacity.setTheunUsedUnitSpace(String.valueOf(theunUsedUnitSpace));
      capacity.setTheUsedUniPerStr(String.valueOf(theUsedUniPerStr));
      capacity.setTheunUsedUniPerStr(String.valueOf(theunUsedUniPerStr));

      logger.info(
          "get the each value theUsedUnitSpace:{}, theunUsedUnitSpace: {}, theUsedUniPerStr: {}, "
              + "theunUsedUniPerStr: {}",
          theUsedUnitSpace, theunUsedUnitSpace, theUsedUniPerStr, theunUsedUniPerStr);

    } catch (Exception e) {
      logger.error("in getSystemCapacity, caught an exception: {}", e);
      capacity.setMessage(e.toString());
    }

    logger.debug("in getSystemCapacity, get capacity: {}", capacity);
    return capacity;
  }

  private double getAllVolumeUnitUsedInfo() {

    long usedSpace = 0L;
    for (InstanceMetadata instance : storageStore.list()) {
      for (RawArchiveMetadata archiveMetadata : instance.getArchives()) {
        usedSpace += archiveMetadata.getUsedSpace();
      }
    }

    logger.warn("get getAllVolumeUsedInfo :{}", usedSpace);
    return (double) (usedSpace) / gbSize;
  }

  private Capacity getSystemCapacityFromInfocenter(long accountId) {
    GetCapacityRequest request = new GetCapacityRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setAccountId(accountId);
    GetCapacityResponse response = null;

    try {
      response = getCapacity(request);
    } catch (TException e) {
      logger.error("in getSystemCapacityFromInfocenter, we get an error: {}", e);
    }

    Capacity capacity = null;
    if (response != null) {
      capacity = new Capacity();
      if (response.getCapacity() < 0 || response.getLogicalCapacity() < 0
          || response.getFreeSpace() < 0
          || response.getCapacity() < response.getFreeSpace()) {
        logger.warn("system capacity is strange: totalSpace:{} freeSpace:{} availableSpace:{}",
            response.getCapacity(), response.getFreeSpace(), response.getLogicalCapacity());
      }

      if (response.getCapacity() < response.getFreeSpace()) {
        capacity.setTotalCapacity(String.valueOf(response.getFreeSpace()));
      } else {
        capacity.setTotalCapacity(String.valueOf(response.getCapacity()));
      }

      capacity.setAvailableCapacity(String.valueOf(response.getLogicalCapacity()));
      capacity.setFreeSpace(String.valueOf(response.getFreeSpace()));
    }

    logger.debug("in getSystemCapacityFromInfocenter, get capacity :{}", capacity);
    return capacity;
  }

  private InstanceStatusStatistics getInstanceStatusStatistics() {
    InstanceStatusStatistics instanceStatusStatistics = new InstanceStatusStatistics();
    // get instance statistics
    try {
      List<Instance> instanceList = getAllInstances();
      int serviceHealthy = 0;
      int serviceSick = 0;
      int serviceFailed = 0;
      final int serviceTotal = instanceList.size();

      for (Instance instance : instanceList) {
        if (InstanceStatus.HEALTHY.name().equals(
            instance.getStatus().name()) || InstanceStatus.SUSPEND.name()
            .equals(instance.getStatus().name())) {
          serviceHealthy++;
        } else if (InstanceStatus.SICK.name().equalsIgnoreCase(instance.getStatus().name())) {
          serviceSick++;
        } else {
          serviceFailed++;
        }

      }

      instanceStatusStatistics.setServiceHealthy(serviceHealthy);
      instanceStatusStatistics.setServiceSick(serviceSick);
      instanceStatusStatistics.setServiceFailed(serviceFailed);
      instanceStatusStatistics.setServiceTotal(serviceTotal);
    } catch (Exception e) {
      logger.error("in getInstanceStatusStatistics, caught an exception ", e);
      instanceStatusStatistics.setMessage(e.toString());
    }

    logger.debug("in getInstanceStatusStatistics, the instanceStatusStatistics is :{}",
        instanceStatusStatistics);
    return instanceStatusStatistics;
  }


  
  public List<Instance> getAllInstances() throws Exception {
    List<Instance> instanceList = new ArrayList<>();
    List<String> dihHosts = new ArrayList<>();

    try {
      for (Instance instance : instanceStore.getAll()) {
        if ((instance.getName().equals(PyService.DIH.getServiceName())) && instance.getStatus()
            .name()
            .endsWith(InstanceStatus.HEALTHY.name())) {
          dihHosts.add(instance.getEndPoint().getHostName());
        }
        if (!instance.getName().equals(PyService.COORDINATOR.getServiceName())) {
          instanceList.add(instance);
        }
      }

      for (Instance instance : instanceList) {
        if (!dihHosts.contains(instance.getEndPoint().getHostName()) && !instance.getName()
            .equals("DIH")) {
          // warning: this FORGOTTEN is a alternative for UNKNOWN.
          instance.setStatus(InstanceStatus.DISUSED);
        }

        logger.debug("in getAllInstances, get all instance is {}", instance);
      }

    } catch (Exception e) {
      logger.error("in getAllInstances, catach an Exception", e);
      throw e;
    }

    logger.debug("in getAllInstances, get instanceList :{}", instanceList);
    return instanceList;
  }

  private ClientTotal getTotalClient(long accountId) {
    // get total client.
    ClientTotal clientTotal = new ClientTotal();
    try {
      ListVolumeAccessRulesRequest listVolumeAccessRulesRequest = new ListVolumeAccessRulesRequest(
          RequestIdBuilder.get(), accountId);
      ListVolumeAccessRulesResponse listVolumeAccessRulesResponse = listVolumeAccessRules(
          listVolumeAccessRulesRequest);

      int accessRulesSize = 0;
      if (listVolumeAccessRulesResponse != null
          && listVolumeAccessRulesResponse.getAccessRules() != null
          && listVolumeAccessRulesResponse.getAccessRulesSize() > 0) {
        accessRulesSize = listVolumeAccessRulesResponse.getAccessRulesSize();
      }

      ListIscsiAccessRulesRequest request = new ListIscsiAccessRulesRequest();
      ListIscsiAccessRulesResponse response = new ListIscsiAccessRulesResponse();
      request.setRequestId(RequestIdBuilder.get());
      request.setAccountId(accountId);

      ListIscsiAccessRulesResponse listIscsiAccessRulesResponse = listIscsiAccessRules(request);
      int iscsiAccessRulesSize = 0;
      if (listIscsiAccessRulesResponse != null && listIscsiAccessRulesResponse.isSetAccessRules()) {
        iscsiAccessRulesSize = listIscsiAccessRulesResponse.getAccessRulesSize();
      }

      clientTotal.setClientTotal(accessRulesSize + iscsiAccessRulesSize);
    } catch (Exception e) {
      logger.error("in getTotalClient, caught an exception ", e);
      clientTotal.setMessage(e.toString());
    }

    logger.debug("in getTotalClient, get the clientTotal :{}", clientTotal);
    return clientTotal;
  }

  private DiskStatistics getDiskStatistics() {
    DiskStatistics diskStatistics = new DiskStatistics();

    try {
      int goodDiskCount = 0;
      long badDiskCount = 0;
      long allDiskCount = 0;
      ListArchivesRequestThrift request = new ListArchivesRequestThrift();
      request.setRequestId(RequestIdBuilder.get());

      ListArchivesResponseThrift listArchivesResponseThrift = listArchives(request);
      logger.debug("in getDiskStatistics, listInstanceMetadata response {}",
          listArchivesResponseThrift);

      List<InstanceMetadataThrift> instanceMetadataThrifts = listArchivesResponseThrift
          .getInstanceMetadata();

      if (instanceMetadataThrifts != null && instanceMetadataThrifts.size() != 0) {
        for (InstanceMetadataThrift instanceMetadataThrift : instanceMetadataThrifts) {

          GetArchivesRequestThrift getArchivesRequestThrift = new GetArchivesRequestThrift();
          getArchivesRequestThrift.setRequestId(RequestIdBuilder.get());
          getArchivesRequestThrift.setInstanceId(instanceMetadataThrift.getInstanceId());
          InstanceMetadataThrift instanceMetadataThriftGet = null;

          InstanceMetadata instance = storageStore.get(instanceMetadataThrift.getInstanceId());
          if (null != instance) {
            instanceMetadataThriftGet = RequestResponseHelper.buildThriftInstanceFrom(instance);

            for (ArchiveMetadataThrift archiveMetadataThrift : instanceMetadataThriftGet
                .getArchiveMetadata()) {
              allDiskCount++;
              if (archiveMetadataThrift.getStatus().equals(ArchiveStatusThrift.OFFLINING)
                  || archiveMetadataThrift.getStatus().equals(ArchiveStatusThrift.GOOD)
                  || archiveMetadataThrift.getStatus().equals(ArchiveStatusThrift.OFFLINED)) {
                goodDiskCount++;
              } else {
                badDiskCount++;
              }
             
             
            }
          }
        }
      }

      diskStatistics.setGoodDiskCount(goodDiskCount);
      diskStatistics.setBadDiskCount(badDiskCount);
      diskStatistics.setAllDiskCount(allDiskCount);
    } catch (Exception e) {
      logger.error("in getDiskStatistics, caught an exception ", e);
      diskStatistics.setMessage(e.toString());
    }

    logger.debug("in getDiskStatistics, get diskStatistics :{}", diskStatistics);
    return diskStatistics;
  }

  private ServerNodeStatistics getServerNodeStatistics(long accountId) {
    ServerNodeStatistics serverNodeStatistics = new ServerNodeStatistics();

    try {
      ListServerNodesRequestThrift request = new ListServerNodesRequestThrift();
      request.setRequestId(RequestIdBuilder.get());
      request.setAccountId(accountId);
      request.setLimit(100000);
      logger.debug("in getServerNodeStatistics, listServernodes request is {}", request);

      ListServerNodesResponseThrift listServerNodesResponseThrift = listServerNodes(request);
      logger.debug("in getServerNodeStatistics, listServernodes response is {}",
          listServerNodesResponseThrift);

      int okServerNodeCounts = 0;
      int unknownServerNodeCount = 0;
      int totalServerNodeCount = 0;
      if (listServerNodesResponseThrift != null
          && listServerNodesResponseThrift.getServerNodesList().size() != 0) {
        totalServerNodeCount = listServerNodesResponseThrift.getServerNodesListSize();
        for (ServerNodeThrift nodeThrift : listServerNodesResponseThrift.getServerNodesList()) {
          if (nodeThrift.getStatus().equals("ok")) {
            okServerNodeCounts++;
          } else {
            unknownServerNodeCount++;
          }
        }
      }

      serverNodeStatistics.setOkServerNodeCounts(okServerNodeCounts);
      serverNodeStatistics.setUnknownServerNodeCount(unknownServerNodeCount);
      serverNodeStatistics.setTotalServerNodeCount(totalServerNodeCount);

    } catch (Exception e) {
      logger.error("in getServerNodeStatistics, caught an exception ", e);
      serverNodeStatistics.setMessage(e.toString());
    }

    logger.debug("in getServerNodeStatistics, get serverNodeStatistics :{}", serverNodeStatistics);
    return serverNodeStatistics;
  }

  public void startAutoRebalance() throws TException {
    logger.warn("enable rebalance!");
    segmentUnitsDistributionManager.setRebalanceEnable(true);
  }

  private PoolStatistics getStorageStatistics(GetDashboardInfoRequest request) {
    ListStoragePoolRequestThrift listStoragePoolRequestThrift = new ListStoragePoolRequestThrift();
    listStoragePoolRequestThrift.setRequestId(RequestIdBuilder.get());
    listStoragePoolRequestThrift.setAccountId(request.getAccountId());

    ListStoragePoolResponseThrift listStoragePoolResponseThrift = null;
    PoolStatistics poolStatistics = new PoolStatistics();
    try {
      listStoragePoolResponseThrift = listStoragePools(listStoragePoolRequestThrift);
    } catch (TException e) {
      logger.error("in getStorageStatistics, get an error: {}", e);
      poolStatistics.setMessage(e.toString());
    }
    List<OneStoragePoolDisplayThrift> storagePoolDisplays = null;
    if (listStoragePoolResponseThrift != null) {
      storagePoolDisplays = listStoragePoolResponseThrift.getStoragePoolDisplays();
    }
    int poolHigh = 0;
    int poolMiddle = 0;
    int poolLow = 0;
    int poolTotal = 0;
    if (storagePoolDisplays != null) {
      for (OneStoragePoolDisplayThrift storagePoolDisplay : storagePoolDisplays) {
        StoragePoolThrift storagePoolThrift = storagePoolDisplay.getStoragePoolThrift();
        String storagePoolLevel = storagePoolThrift.getStoragePoolLevel();
        poolTotal++;
        switch (storagePoolLevel) {
          case "HIGH":
            poolHigh++;
            break;
          case "MIDDLE":
            poolMiddle++;
            break;
          case "LOW":
            poolLow++;
            break;
          default:
            break;
        }
      }
    }
    poolStatistics.setPoolTotal(poolTotal);
    poolStatistics.setPoolHigh(poolHigh);
    poolStatistics.setPoolMiddle(poolMiddle);
    poolStatistics.setPoolLow(poolLow);

    logger.debug("in getStorageStatistics, get poolStatistics :{}", poolStatistics);
    return poolStatistics;
  }

  private VolumeCounts getVolumeStatistics(long accountId) {
    // get volume statistics
    VolumeCounts returnVolumeCounts = retrieveVolumeCounts(accountId);
    if (returnVolumeCounts == null) {
      logger.error("in getVolumeStatistics, caught an exception returnVolumeCounts is null.");
      VolumeCounts volumeCounts = new VolumeCounts();
      volumeCounts
          .setMessage("in getVolumeStatistics, caught an exception returnVolumeCounts is null.");
      return volumeCounts;
    } else {
      logger.info("in getVolumeStatistics, get volumeCounts :{}", returnVolumeCounts);
      return returnVolumeCounts;
    }
  }

  private VolumeCounts retrieveVolumeCounts(long accountId) {
    VolumeCounts volumeCounts = new VolumeCounts();

    List<VolumeAccessRuleThrift> volumeAccessRuleList = listVolumeAccessRules(accountId);
    volumeCounts.setTotalClients(volumeAccessRuleList.size());

    //list volume
    ListVolumesRequest request = new ListVolumesRequest();
    request.setAccountId(accountId);
    request.setRequestId(RequestIdBuilder.get());
    request.setContainDeadVolume(false);

    ListVolumesResponse response = new ListVolumesResponse();
    try {
      response = listVolumes(request);
    } catch (TException e) {
      logger.error("when retrieveVolumeCounts to list volume, find error: ", e);
      return null;
    }

    if (response != null) {
      List<VolumeMetadataThrift> volumeList = response.getVolumes();
      int countok = 0;
      int countDegree = 0;
      int countUnavailable = 0;
      int clientNums = 0;
      for (VolumeMetadataThrift volumeMetadataThrift : volumeList) {
        if (volumeMetadataThrift.getName()
            .contains(MOVE_VOLUME_APPEND_STRING_FOR_ORIGINAL_VOLUME)) {
          continue;
        }

        String volumeStatus;
        if (volumeMetadataThrift.getInAction().equals(VolumeInActionThrift.NULL)) {
          volumeStatus = volumeMetadataThrift.getVolumeStatus().name();
        } else {
          volumeStatus = volumeMetadataThrift.getInAction().name();
        }

        if (volumeStatus.equals("Unavailable")) {
          countUnavailable++;
        } else {
          countok++;
        }

        //get Drivers
        List<DriverMetadata> volumeBindingDrivers = driverStore
            .get(volumeMetadataThrift.getVolumeId());
        if (volumeBindingDrivers != null && volumeBindingDrivers.size() > 0) {
          for (DriverMetadata driverMetadata : volumeBindingDrivers) {
            if (driverMetadata.getClientHostAccessRule() != null) {
              clientNums += driverMetadata.getClientHostAccessRule().size();
            }
          }
        }
      }

      volumeCounts.setDegreeCounts(countDegree);
      volumeCounts.setOkCounts(countok);
      volumeCounts.setUnavailableCounts(countUnavailable);
      volumeCounts.setConnectedClients(clientNums);
    } else {
      return null;
    }

    logger.info("in retrieveVolumeCounts, get volumeCounts :{}", volumeCounts);
    return volumeCounts;
  }

  public boolean listContentEquals(List list1, List list2) {
    if (list1 == null && list2 == null) {
      return true;
    }

    if (list1 == null && list2 != null) {
      return false;
    }

    if (list1 != null && list2 == null) {
      return false;
    }

    if (list1.size() != list2.size()) {
      return false;
    } else {
      for (Object object : list1) {
        if (!list2.contains(object)) {
          return false;
        }
      }
    }

    return true;
  }

  
  public long buildActiveOperationAndSaveToDb(Long accountId, Long targetId,
      OperationType operationType,
      TargetType targetType, String operationObject, String targetName, Long targetSourceSize) {
    return buildOperationAndSaveToDb(accountId, targetId, operationType, targetType,
        operationObject, targetName,
        OperationStatus.ACTIVITING, targetSourceSize, null, null);
  }


  
  public void buildActiveOperationWrtLaunchDriverAndSaveToDb(Long accountId, Long targetId,
      OperationType operationType, String operationObject, String targetName,
      List<EndPoint> endPoint) {
    buildOperationAndSaveToDb(accountId, targetId, operationType, TargetType.DRIVER,
        operationObject, targetName,
        OperationStatus.ACTIVITING, 0L, endPoint, null);
  }


  
  public void buildFailedOperationWrtLaunchDriverAndSaveToDb(Long accountId, Long targetId,
      OperationType operationType, String operationObject, String targetName,
      List<EndPoint> endPoint) {
    buildOperationAndSaveToDb(accountId, targetId, operationType, TargetType.DRIVER,
        operationObject, targetName,
        OperationStatus.FAILED, 0L, endPoint, null);
  }


  
  public void buildEndOperationWrtUmountDriverAndSaveToDb(Long accountId, Long targetId,
      OperationType operationType,
      String operationObject, String targetName, List<EndPoint> endPoint) {
    buildOperationAndSaveToDb(accountId, targetId, operationType, TargetType.DRIVER,
        operationObject, targetName,
        OperationStatus.SUCCESS, 0L, endPoint, null);
  }

  private void buildEndOperationWrtApplyVolumeAccessRulesAndSaveToDb(long accountId, long volumeId,
      String operationObject, String targetName) {
    buildOperationAndSaveToDb(accountId, volumeId, OperationType.APPLY, TargetType.ACCESSRULE,
        operationObject,
        targetName, OperationStatus.SUCCESS, 0L, null, null);
  }

  private void buildEndOperationWrtCancelVolumeAccessRulesAndSaveToDb(long accountId, long volumeId,
      String operationObject, String targetName) {
    buildOperationAndSaveToDb(accountId, volumeId, OperationType.CANCEL, TargetType.ACCESSRULE,
        operationObject,
        targetName, OperationStatus.SUCCESS, 0L, null, null);
  }

  private void buildEndOperationWrtDeleteVolumeAccessRulesAndSaveToDb(long accountId,
      String operationObject) {
    buildOperationAndSaveToDb(accountId, null, OperationType.DELETE, TargetType.ACCESSRULE,
        operationObject, "",
        OperationStatus.SUCCESS, 0L, null, null);
  }

  private void buildEndOperationWrtCreateVolumeAccessRulesAndSaveToDb(long accountId,
      String targetName) {
    buildOperationAndSaveToDb(accountId, null, OperationType.CREATE, TargetType.ACCESSRULE, "",
        targetName,
        OperationStatus.SUCCESS, 0L, null, null);
  }

  private void buildEndOperationWrtCreateDomainAndSaveToDb(long accountId, String targetName) {
    buildOperationAndSaveToDb(accountId, null, OperationType.CREATE, TargetType.DOMAIN, "",
        targetName,
        OperationStatus.SUCCESS, 0L, null, null);
  }

  private void buildEndOperationWrtModifyDomainAndSaveToDb(long accountId, String operationObject) {
    buildOperationAndSaveToDb(accountId, null, OperationType.MODIFY, TargetType.DOMAIN,
        operationObject, "",
        OperationStatus.SUCCESS, 0L, null, null);
  }

  private void buildEndOperationWrtCreateStoragePoolAndSaveToDb(long accountId, String targetName) {
    buildOperationAndSaveToDb(accountId, null, OperationType.CREATE, TargetType.STORAGEPOOL, "",
        targetName,
        OperationStatus.SUCCESS, 0L, null, null);
  }

  private void buildEndOperationWrtModifyStoragePoolAndSaveToDb(long accountId,
      String operationObject) {
    buildOperationAndSaveToDb(accountId, null, OperationType.MODIFY, TargetType.STORAGEPOOL,
        operationObject, "",
        OperationStatus.SUCCESS, 0L, null, null);
  }

  private void buildEndOperationWrtCreateRoleAndSaveToDb(long accountId, String targetName) {
    buildOperationAndSaveToDb(accountId, null, OperationType.CREATE, TargetType.ROLE, "",
        targetName,
        OperationStatus.SUCCESS, 0L, null, null);
  }

  private void buildEndOperationWrtResetPasswordAndSaveToDb(long accountId,
      String operationObject) {
    buildOperationAndSaveToDb(accountId, null, OperationType.RESET, TargetType.PASSWORD,
        operationObject, "",
        OperationStatus.SUCCESS, 0L, null, null);
  }

  private void buildEndOperationWrtDeleteRoleAndSaveToDb(long accountId, String operationObject) {
    buildOperationAndSaveToDb(accountId, null, OperationType.DELETE, TargetType.ROLE,
        operationObject, "",
        OperationStatus.SUCCESS, 0L, null, null);
  }

  private void buildEndOperationWrtModifyRoleAndSaveToDb(long accountId, String operationObject) {
    buildOperationAndSaveToDb(accountId, null, OperationType.MODIFY, TargetType.ROLE,
        operationObject, "",
        OperationStatus.SUCCESS, 0L, null, null);
  }

  private void buildEndOperationWrtCreateUserAndSaveToDb(long accountId, String targetName) {
    buildOperationAndSaveToDb(accountId, null, OperationType.CREATE, TargetType.USER, "",
        targetName,
        OperationStatus.SUCCESS, 0L, null, null);
  }

  private void buildEndOperationWrtDeleteUserAndSaveToDb(long accountId, String operationObject) {
    buildOperationAndSaveToDb(accountId, null, OperationType.DELETE, TargetType.USER,
        operationObject, "",
        OperationStatus.SUCCESS, 0L, null, null);
  }

  private void buildEndOperationWrtUserLoginAndSaveToDb(long accountId, String operationObject) {
    buildOperationAndSaveToDb(accountId, null, OperationType.LOGIN, TargetType.USER,
        operationObject, "",
        OperationStatus.SUCCESS, 0L, null, null);
  }

  private void buildEndOperationWrtUserLogoutAndSaveToDb(long accountId, String operationObject) {
    buildOperationAndSaveToDb(accountId, null, OperationType.LOGOUT, TargetType.USER,
        operationObject, "",
        OperationStatus.SUCCESS, 0L, null, null);
  }


  
  public void buildEndOperationSuccessScsiUmountDriverAndSaveToDb(Long accountId, Long targetId,
      OperationType operationType, String operationObject, String targetName,
      List<EndPoint> endPoint) {
    buildOperationAndSaveToDb(accountId, targetId, operationType, TargetType.DRIVER,
        operationObject, targetName,
        OperationStatus.SUCCESS, 0L, endPoint, null);
  }


  
  public void buildEndOperationFailedScsiUmountDriverAndSaveToDb(Long accountId, Long targetId,
      OperationType operationType, String operationObject, String targetName,
      List<EndPoint> endPoint) {
    buildOperationAndSaveToDb(accountId, targetId, operationType, TargetType.DRIVER,
        operationObject, targetName,
        OperationStatus.FAILED, 0L, endPoint, null);
  }

  private void buildEndOperationWrtCreateScsiClientAndSaveToDb(long accountId,
      String operationObject) {
    buildOperationAndSaveToDb(accountId, null, OperationType.CREATE, TargetType.SCSI_CLIENT,
        operationObject, "",
        OperationStatus.SUCCESS, 0L, null, null);
  }

  private void buildEndOperationWrtDeleteScsiClientAndSaveToDb(long accountId,
      String operationObject) {
    buildOperationAndSaveToDb(accountId, null, OperationType.DELETE, TargetType.SCSI_CLIENT,
        operationObject, "",
        OperationStatus.SUCCESS, 0L, null, null);
  }

  private void buildEndOperationWrtDeleteErrorScsiClientAndSaveToDb(long accountId,
      String operationObject) {
    buildOperationAndSaveToDb(accountId, null, OperationType.DELETE, TargetType.SCSI_CLIENT,
        operationObject, "",
        OperationStatus.FAILED, 0L, null, null);
  }


  
  public long buildOperationAndSaveToDb(Long accountId, Long targetId, OperationType operationType,
      TargetType targetType, String operationObject, String targetName,
      OperationStatus operationStatus,
      Long targetSourceSize, List<EndPoint> endPointList, Integer snapshotId) {
    final long operationId = RequestIdBuilder.get();
    Operation operation = new Operation();
    long time = System.currentTimeMillis();
    operation.setStartTime(time);
    operation.setEndTime(time);
    operation.setStatus(operationStatus);
    if (operationStatus == OperationStatus.SUCCESS) {
      operation.setProgress(100L);
    } else {
      operation.setProgress(0L);
    }
    operation.setOperationId(operationId);
    operation.setAccountId(accountId);
    AccountMetadata account = securityManager.getAccountById(accountId);
    if (account != null) {
      operation.setAccountName(account.getAccountName());
    }
    operation.setTargetId(targetId);
    operation.setOperationType(operationType);
    operation.setTargetType(targetType);
    operation.setOperationObject(operationObject);
    operation.setTargetName(targetName);
    operation.setTargetSourceSize(targetSourceSize);
    operation.setEndPointList(endPointList);
    operation.setSnapshotId(snapshotId);
    try {
      operationStore.saveOperation(operation);
      logger.warn("buildOperationAndSaveToDB operation: {}", operation);
    } catch (Exception e) {
      logger.error("save operation {} to database fail", operation, e);
    }
    return operationId;
  }

  /**
   * creat volume information and save to db.
   **/
  protected void makeVolumeCreationRequestAndcreateVolume(VolumeCreationRequest volumeRequest)
      throws NotEnoughSpaceExceptionThrift, InvalidInputExceptionThrift,
      VolumeNameExistedExceptionThrift,
      VolumeExistingExceptionThrift, StoragePoolNotExistInDoaminExceptionThrift,
      DomainNotExistedExceptionThrift, StoragePoolNotExistedExceptionThrift,
      DomainIsDeletingExceptionThrift,
      StoragePoolIsDeletingExceptionThrift, TException {

    logger.warn("saveCreateOrExtendVolumeRequest request: {}", volumeRequest);

    CreateVolumeRequest createVolumeRequest = new CreateVolumeRequest();
    createVolumeRequest.setRequestId(RequestIdBuilder.get());
    createVolumeRequest.setRootVolumeId(volumeRequest.getRootVolumeId());
    createVolumeRequest.setVolumeId(volumeRequest.getVolumeId());
    createVolumeRequest.setName(volumeRequest.getName());
    createVolumeRequest.setVolumeSize(volumeRequest.getVolumeSize());
    createVolumeRequest
        .setVolumeType(VolumeType.valueOf(volumeRequest.getVolumeType()).getVolumeTypeThrift());
    createVolumeRequest.setSegmentSize(volumeRequest.getSegmentSize());
    createVolumeRequest.setDomainId(volumeRequest.getDomainId());
    createVolumeRequest.setStoragePoolId(volumeRequest.getStoragePoolId());
    createVolumeRequest.setAccountId(volumeRequest.getAccountId());
    createVolumeRequest.setRequestType(volumeRequest.getRequestType().name());
    createVolumeRequest.setEnableLaunchMultiDrivers(volumeRequest.isEnableLaunchMultiDrivers());
    createVolumeRequest.setVolumeDescription(volumeRequest.getVolumeDescription());

    // when clone volume from a snapshot, add cloningVolumeId and cloningSnapshotId to volume;
    VolumeCreationRequest.RequestType requestType = volumeRequest.getRequestType();

    if (requestType == VolumeCreationRequest.RequestType.EXTEND_VOLUME) {
      //extend volume
      createAndSaveVolumeInformationForExtendVolume(createVolumeRequest);
    } else {
      //crete volume
      createAndSaveVolumeInformation(createVolumeRequest, volumeRequest);
    }
  }

  public synchronized void createAndSaveVolumeInformation(CreateVolumeRequest request,
      VolumeCreationRequest volumeRequest)
      throws NotEnoughSpaceExceptionThrift, InvalidInputExceptionThrift,
      VolumeNameExistedExceptionThrift,
      VolumeExistingExceptionThrift, StoragePoolNotExistInDoaminExceptionThrift,
      DomainNotExistedExceptionThrift, StoragePoolNotExistedExceptionThrift,
      DomainIsDeletingExceptionThrift,
      StoragePoolIsDeletingExceptionThrift, TException {
    logger.warn("CreateVolume to db request: {}", request);
    if (request.getName() == null || request.getName().length() > 100) {
      logger.error("volume name is null or too long");
      throw new InvalidInputExceptionThrift();
    }
    if (!request.isSetDomainId() || !request.isSetStoragePoolId()) {
      logger
          .error("domainId or storage pool id is not set when create volume, request:{}", request);
      throw new InvalidInputExceptionThrift();
    }
    Domain domain = null;
    try {
      domain = domainStore.getDomain(request.getDomainId());
    } catch (Exception e) {
      logger.warn("can not get domain", e);
    }
    if (domain == null) {
      logger.error("domain:{} is not exist", request.getDomainId());
      throw new DomainNotExistedExceptionThrift();
    }
    if (domain.isDeleting()) {
      logger.error("domain:{} is deleting, can not create volume on it", domain);
      throw new DomainIsDeletingExceptionThrift();
    }
    StoragePool storagePool = null;
    try {
      storagePool = storagePoolStore.getStoragePool(request.getStoragePoolId());
    } catch (Exception e) {
      logger.warn("can not get storage pool", e);
    }
    if (storagePool == null) {
      logger.error("storage pool:{} is not exist when create volume", request.getStoragePoolId());
      throw new StoragePoolNotExistedExceptionThrift();
    }
    if (storagePool.isDeleting()) {
      logger.error("storagePool:{} is deleting, can not create volume on it", storagePool);
      throw new StoragePoolIsDeletingExceptionThrift();
    }
    if (!domain.getStoragePools().contains(storagePool.getPoolId())) {
      logger.error("domain:{} does not contain storage pool:{}", domain, storagePool);
      throw new StoragePoolNotExistInDoaminExceptionThrift();
    }

   
   
   
   
   
   
   

    long expectedSize = request.getVolumeSize();
    if (expectedSize < segmentSize) {
      logger.error("volume size : {} less than segment size : {}", expectedSize, segmentSize);
      throw new InvalidInputExceptionThrift();
    }

    VolumeType volumeType = RequestResponseHelper.convertVolumeType(request.getVolumeType());
    int numberOfMembers = volumeType.getNumMembers() - volumeType.getNumArbiters();

    // calculate the total capacity, should not exceed the threshold
    long freeSpace = 0L;
    for (InstanceMetadata entry : storageStore.list()) {
      if (entry.getDatanodeStatus().equals(OK)) {
        freeSpace += entry.getFreeSpace();
      }
    }

    if (refreshTimeAndFreeSpace.getLastRefreshTime() == 0 || (
        System.currentTimeMillis() - refreshTimeAndFreeSpace.getLastRefreshTime()
            >= InfoCenterConstants
            .getRefreshPeriodTime())) {
      refreshTimeAndFreeSpace.setActualFreeSpace(freeSpace);
      refreshTimeAndFreeSpace.setLastRefreshTime(System.currentTimeMillis());
    }

   
   
    if (refreshTimeAndFreeSpace.getActualFreeSpace() - numberOfMembers * expectedSize >= 0) {
      refreshTimeAndFreeSpace
          .setActualFreeSpace(
              refreshTimeAndFreeSpace.getActualFreeSpace() - numberOfMembers * expectedSize);
    } else {
      logger.warn("Actual free space is not enough space:{} user apply space:{}",
          refreshTimeAndFreeSpace.getActualFreeSpace(), numberOfMembers * expectedSize);
      throw new NotEnoughSpaceExceptionThrift();
    }

    VolumeMetadata volumeMetadata = volumeStore.getVolume(request.getVolumeId());

    if (null != volumeMetadata) {
      logger.error("volume with same id {} has existed", request.getVolumeId());
      throw new VolumeExistingExceptionThrift();
    }

   
   
    String eachTimeExtendVolumeSize =
        String.valueOf((int) Math.ceil(expectedSize * 1.0 / segmentSize)) + ",";
    long srcVolumeId = volumeRequest.getSrcVolumeId();
    if (srcVolumeId != 0) {
      VolumeMetadata srcVolumeMetadata = volumeStore.getVolume(srcVolumeId);
      if (srcVolumeMetadata != null) {
        eachTimeExtendVolumeSize = srcVolumeMetadata.getEachTimeExtendVolumeSize();
      } else {
        logger.error("when in :{}, for volume :{}, can not find the src volume :{}",
            request.getRequestType(), request.getVolumeId(),
            srcVolumeId);
      }
    }
    volumeMetadata = new VolumeMetadata(request.getRootVolumeId(), request.getVolumeId(),
        request.getVolumeSize(),
        request.getSegmentSize(), volumeType, request.getDomainId(),
        request.getStoragePoolId());

    volumeMetadata.setName(request.getName());
    volumeMetadata.setAccountId(request.getAccountId());
    volumeMetadata.setSegmentNumToCreateEachTime(request.getLeastSegmentUnitCount());
    volumeMetadata.initVolumeLayout();
    volumeMetadata.setVolumeCreatedTime(new Date());
    volumeMetadata.setVolumeSource(buildVolumeSourceTypeFrom(request.getRequestType()));
    volumeMetadata.setReadWrite(VolumeMetadata.ReadWriteType.READWRITE);
    volumeMetadata.setPageWrappCount(pageWrappCount);
    volumeMetadata.setSegmentWrappCount(segmentWrappCount);
    volumeMetadata.setEnableLaunchMultiDrivers(request.isEnableLaunchMultiDrivers());
    volumeMetadata.setVolumeDescription(request.getVolumeDescription());
    //set the default
    volumeMetadata.setClientLastConnectTime(defaultNotClientConnect);

    //make the real create volume size for extend volume
    volumeMetadata.setEachTimeExtendVolumeSize(eachTimeExtendVolumeSize);

    logger.info("get the getVolumeSource :{} ", volumeMetadata.getVolumeSource());
    switch (volumeMetadata.getVolumeSource()) {
      case CREATE_VOLUME:
        volumeMetadata.setInAction(CREATING);
        break;
      default:
        break;
    }

    if (request.getDomainId() == 0) {
      volumeMetadata.setDomainId(null);
    } else {
      volumeMetadata.setDomainId(request.getDomainId());
    }

    // volume need to check volume with same name exists
    if (null != volumeStore.getVolumeNotDeadByName(volumeMetadata.getName())) {
      logger.error("some volume has been exist, name is {}", volumeMetadata.getName());
      throw new VolumeNameExistedExceptionThrift();
    }

    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> segIndex = null;
    segIndex = segmentUnitsDistributionManager
        .reserveVolume(expectedSize, volumeMetadata.getVolumeType(),
            false,
            segmentWrappCount, volumeMetadata.getStoragePoolId());
    if (segIndex != null) {
      volumeStore.saveVolume(volumeMetadata);
      try {
        storagePoolStore
            .addVolumeId(volumeMetadata.getStoragePoolId(), volumeMetadata.getVolumeId());
      } catch (Exception e) {
        logger.error("can not add: {} to storage pool: {}", volumeMetadata.getVolumeId(),
            volumeMetadata.getStoragePoolId(), e);
        throw new StoragePoolNotExistedExceptionThrift();
      }

      logger.warn("CreateVolume to db ok, the volume id is :{}", request.getVolumeId());
    } else {
      logger.error("no space for create volume:{}, volume size:{}, current free space is {}",
          volumeMetadata.getName(), volumeMetadata.getVolumeSize(), freeSpace);
      throw new NotEnoughSpaceExceptionThrift();
    }
  }


  
  private DataNodeService.Iface getSyncClientToDataNode(InstanceId dataNodeInstanceId)
      throws GenericThriftClientFactoryException {
    Instance dataNodeInstance = instanceStore.get(dataNodeInstanceId);
    if (dataNodeInstance == null) {
      logger.warn("Can't get data node intance for {}", dataNodeInstanceId);
      return null;
    }

    return dataNodeClientFactory.generateSyncClient(dataNodeInstance.getEndPoint(), timeout);
  }


  
  public synchronized void createAndSaveVolumeInformationForExtendVolume(
      CreateVolumeRequest request)
      throws NotEnoughSpaceExceptionThrift, InvalidInputExceptionThrift,
      RootVolumeNotFoundExceptionThrift,
      StoragePoolNotExistInDoaminExceptionThrift, DomainNotExistedExceptionThrift,
      StoragePoolNotExistedExceptionThrift, DomainIsDeletingExceptionThrift,
      StoragePoolIsDeletingExceptionThrift, VolumeIsCloningExceptionThrift, TException {

    logger.warn("createAndSaveVolumeInformationForExtendVolume, CreateVolumeRequest request: {}",
        request);

    if (request.getName() == null || request.getName().length() > 100) {
      logger.error("volume name is null or too long");
      throw new InvalidInputExceptionThrift();
    }
    if (!request.isSetDomainId() || !request.isSetStoragePoolId()) {
      logger
          .error("domainId or storage pool id is not set when create volume, request:{}", request);
      throw new InvalidInputExceptionThrift();
    }
    Domain domain = null;
    try {
      domain = domainStore.getDomain(request.getDomainId());
    } catch (Exception e) {
      logger.warn("can not get domain", e);
    }
    if (domain == null) {
      logger.error("domain:{} is not exist", request.getDomainId());
      throw new DomainNotExistedExceptionThrift();
    }
    if (domain.isDeleting()) {
      logger.error("domain:{} is deleting, can not create volume on it", domain);
      throw new DomainIsDeletingExceptionThrift();
    }
    StoragePool storagePool = null;
    try {
      storagePool = storagePoolStore.getStoragePool(request.getStoragePoolId());
    } catch (Exception e) {
      logger.warn("can not get storage pool", e);
    }
    if (storagePool == null) {
      logger.error("storage pool:{} is not exist when create volume", request.getStoragePoolId());
      throw new StoragePoolNotExistedExceptionThrift();
    }
    if (storagePool.isDeleting()) {
      logger.error("storagePool:{} is deleting, can not create volume on it", storagePool);
      throw new StoragePoolIsDeletingExceptionThrift();
    }
    if (!domain.getStoragePools().contains(storagePool.getPoolId())) {
      logger.error("domain:{} does not contain storage pool:{}", domain, storagePool);
      throw new StoragePoolNotExistInDoaminExceptionThrift();
    }

    long expectedSize = request.getVolumeSize();
    if (expectedSize < segmentSize) {
      logger.error("volume size : {} less than segment size : {}", expectedSize, segmentSize);
      throw new InvalidInputExceptionThrift();
    }

    VolumeType volumeType = RequestResponseHelper.convertVolumeType(request.getVolumeType());
    int numberOfMembers = volumeType.getNumMembers() - volumeType.getNumArbiters();

    // calculate the total capacity, should not exceed the threshold
    long freeSpace = 0L;
    for (InstanceMetadata entry : storageStore.list()) {
      if (entry.getDatanodeStatus().equals(OK)) {
        freeSpace += entry.getFreeSpace();
      }
    }

    if (refreshTimeAndFreeSpace.getLastRefreshTime() == 0 || (
        System.currentTimeMillis() - refreshTimeAndFreeSpace.getLastRefreshTime()
            >= InfoCenterConstants
            .getRefreshPeriodTime())) {
      refreshTimeAndFreeSpace.setActualFreeSpace(freeSpace);
      refreshTimeAndFreeSpace.setLastRefreshTime(System.currentTimeMillis());
    }

   
   
    if (refreshTimeAndFreeSpace.getActualFreeSpace() - numberOfMembers * expectedSize >= 0) {
      refreshTimeAndFreeSpace
          .setActualFreeSpace(
              refreshTimeAndFreeSpace.getActualFreeSpace() - numberOfMembers * expectedSize);
    } else {
      logger.warn("Actual free space is not enough space:{} user apply space:{}",
          refreshTimeAndFreeSpace.getActualFreeSpace(), numberOfMembers * expectedSize);
      throw new NotEnoughSpaceExceptionThrift();
    }

    long volumeId = request.getVolumeId();

    VolumeMetadata volumeMetadata;
    try {
      volumeMetadata = volumeStore.getVolume(volumeId);
    } catch (Exception e) {
      logger.error("Can't get extend volume {}", volumeId);
      throw new RootVolumeNotFoundExceptionThrift();
    }

    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> segIndex2Ins =
        segmentUnitsDistributionManager
            .reserveVolume(expectedSize, volumeMetadata.getVolumeType(),
                false,
                segmentWrappCount, volumeMetadata.getStoragePoolId());
    if (segIndex2Ins != null) {
      logger.info("Successfully reserved volume, going to save volume");
      try {
        storagePoolStore.addVolumeId(volumeMetadata.getStoragePoolId(), volumeId);
      } catch (Exception e) {
        logger.error("can not add: {} to storage pool: {}", volumeId,
            volumeMetadata.getStoragePoolId(), e);
        throw new StoragePoolNotExistedExceptionThrift();
      }

      logger.warn("extend volume and save to db ok, the volume id is :{}", request.getVolumeId());
    } else {
      logger.error("no space for create volume:{}, volume size:{}, current f" + "ree space is {}",
          volumeMetadata.getName(), volumeMetadata.getVolumeSize(), freeSpace);
      throw new NotEnoughSpaceExceptionThrift();
    }
  }

  public void checkInstanceStatus(String operation)
      throws ServiceHavingBeenShutdownThrift, ServiceIsNotAvailableThrift {
    if (shutDownFlag) {
      throw new ServiceHavingBeenShutdownThrift();
    }

    InstanceStatus instanceStatus = appContext.getStatus();
    if (InstanceStatus.HEALTHY != instanceStatus) {
      logger.warn(
          "this time, the :{} quest only the can master do it, current instance status :{} is not"
              + " allow",
          operation, instanceStatus);
      throw new ServiceIsNotAvailableThrift()
          .setDetail("I am " + instanceStatus + "with " + operation);
    }
  }

  /**
   * set and get.
   *****/
  public void setRebalanceTaskExecutor(RebalanceTaskExecutor rebalanceTaskExecutor) {
    this.rebalanceTaskExecutor = rebalanceTaskExecutor;
  }

  public AccountStore getAccountStore() {
    return accountStore;
  }

  public void setAccountStore(AccountStore accountStore) {
    this.accountStore = accountStore;
  }

  public RoleStore getRoleStore() {
    return roleStore;
  }

  public void setRoleStore(RoleStore roleStore) {
    this.roleStore = roleStore;
  }

  public ResourceStore getResourceStore() {
    return resourceStore;
  }

  public void setResourceStore(ResourceStore resourceStore) {
    this.resourceStore = resourceStore;
  }

  public ApiStore getApiStore() {
    return apiStore;
  }

  public void setApiStore(ApiStore apiStore) {
    this.apiStore = apiStore;
  }

  public int getPageWrappCount() {
    return pageWrappCount;
  }

  public void setPageWrappCount(int pageWrappCount) {
    this.pageWrappCount = pageWrappCount;
  }

  public int getSegmentWrappCount() {
    return segmentWrappCount;
  }

  public void setSegmentWrappCount(int segmentWrappCount) {
    this.segmentWrappCount = segmentWrappCount;
  }

  public InstanceMaintenanceDbStore getInstanceMaintenanceDbStore() {
    return instanceMaintenanceDbStore;
  }

  public void setInstanceMaintenanceDbStore(InstanceMaintenanceDbStore instanceMaintenanceDbStore) {
    this.instanceMaintenanceDbStore = instanceMaintenanceDbStore;
  }

  public VolumeJobStoreDb getVolumeJobStoreDb() {
    return volumeJobStoreDb;
  }

  public void setVolumeJobStoreDb(VolumeJobStoreDb volumeJobStoreDb) {
    this.volumeJobStoreDb = volumeJobStoreDb;
  }

  public DriverContainerSelectionStrategy getDriverContainerSelectionStrategy() {
    return driverContainerSelectionStrategy;
  }

  public void setDriverContainerSelectionStrategy(
      DriverContainerSelectionStrategy driverContainerSelectionStrategy) {
    this.driverContainerSelectionStrategy = driverContainerSelectionStrategy;
  }

  public DriverContainerClientFactory getDriverContainerClientFactory() {
    return driverContainerClientFactory;
  }

  public void setDriverContainerClientFactory(
      DriverContainerClientFactory driverContainerClientFactory) {
    this.driverContainerClientFactory = driverContainerClientFactory;
  }

  public GenericThriftClientFactory<DataNodeService.Iface> getDataNodeClientFactory() {
    return dataNodeClientFactory;
  }

  public void setDataNodeClientFactory(
      GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory) {
    this.dataNodeClientFactory = dataNodeClientFactory;
  }

  public InstanceMaintenanceDbStore getInstanceMaintenanceStore() {
    return instanceMaintenanceStore;
  }

  public void setInstanceMaintenanceStore(InstanceMaintenanceDbStore instanceMaintenanceStore) {
    this.instanceMaintenanceStore = instanceMaintenanceStore;
  }

  public CreateSegmentUnitWorkThread getCreateSegmentUnitWorkThread() {
    return createSegmentUnitWorkThread;
  }

  public void setCreateSegmentUnitWorkThread(
      CreateSegmentUnitWorkThread createSegmentUnitWorkThread) {
    this.createSegmentUnitWorkThread = createSegmentUnitWorkThread;
  }

  public OperationStore getOperationStore() {
    return operationStore;
  }

  public void setOperationStore(OperationStore operationStore) {
    this.operationStore = operationStore;
  }

  public int getTimeout() {
    return timeout;
  }

  public void setTimeout(int timeout) {
    this.timeout = timeout;
  }

  public void setVolumeInformationManger(VolumeInformationManger volumeInformationManger) {
    this.volumeInformationManger = volumeInformationManger;
  }

  public void setInstanceVolumeInEquilibriumManger(
      InstanceVolumeInEquilibriumManger instanceVolumeInEquilibriumManger) {
    this.instanceVolumeInEquilibriumManger = instanceVolumeInEquilibriumManger;
  }

  public PySecurityManager getSecurityManager() {
    return securityManager;
  }

  public void setSecurityManager(PySecurityManager securityManager) {
    this.securityManager = securityManager;
  }

  public void setInfoCenterConfiguration(InfoCenterConfiguration infoCenterConfiguration) {
    this.infoCenterConfiguration = infoCenterConfiguration;
  }

  public InstanceStore getInstanceStore() {
    return instanceStore;
  }

  public void setInstanceStore(InstanceStore instanceStore) {
    this.instanceStore = instanceStore;
  }

  public CreateVolumeManager getCreateVolumeManager() {
    return createVolumeManager;
  }

  public void setCreateVolumeManager(CreateVolumeManager createVolumeManager) {
    this.createVolumeManager = createVolumeManager;
  }

  public InstanceIncludeVolumeInfoManger getInstanceIncludeVolumeInfoManger() {
    return instanceIncludeVolumeInfoManger;
  }

  public void setInstanceIncludeVolumeInfoManger(
      InstanceIncludeVolumeInfoManger instanceIncludeVolumeInfoManger) {
    this.instanceIncludeVolumeInfoManger = instanceIncludeVolumeInfoManger;
  }

  public long getDeadVolumeToRemove() {
    return deadVolumeToRemove;
  }

  public void setDeadVolumeToRemove(long deadVolumeToRemove) {
    this.deadVolumeToRemove = deadVolumeToRemove;
  }

  public int getGroupCount() {
    return groupCount;
  }

  public void setGroupCount(int groupCount) {
    this.groupCount = groupCount;
  }

  public DomainStore getDomainStore() {
    return domainStore;
  }

  public void setDomainStore(DomainStore domainStore) {
    this.domainStore = domainStore;
  }

  public StoragePoolStore getStoragePoolStore() {
    return storagePoolStore;
  }

  public void setStoragePoolStore(StoragePoolStore storagePoolStore) {
    this.storagePoolStore = storagePoolStore;
  }

  public long getNextActionTimeIntervalMs() {
    return nextActionTimeIntervalMs;
  }

  public void setNextActionTimeIntervalMs(long nextActionTimeIntervalMs) {
    this.nextActionTimeIntervalMs = nextActionTimeIntervalMs;
  }

  public CapacityRecordStore getCapacityRecordStore() {
    return capacityRecordStore;
  }

  public void setCapacityRecordStore(CapacityRecordStore capacityRecordStore) {
    this.capacityRecordStore = capacityRecordStore;
  }

  public void setExceptionForOperation(ExceptionForOperation exceptionForOperation) {
    this.exceptionForOperation = exceptionForOperation;
  }

  public void setVolumeStore(VolumeStore volumeStore) {
    this.volumeStore = volumeStore;
  }

  public DriverStore getDriverStore() {
    return driverStore;
  }

  public void setDriverStore(DriverStore driverStore) {
    this.driverStore = driverStore;
  }

  public void setStorageStore(StorageStore storageStore) {
    this.storageStore = storageStore;
  }

  public InformationCenterAppEngine getInformationCenterAppEngine() {
    return informationCenterAppEngine;
  }

  public void setInformationCenterAppEngine(InformationCenterAppEngine informationCenterAppEngine) {
    this.informationCenterAppEngine = informationCenterAppEngine;
  }

  public void setSegmentUnitsDistributionManager(
      SegmentUnitsDistributionManager segmentUnitsDistributionManager) {
    this.segmentUnitsDistributionManager = segmentUnitsDistributionManager;
  }

  public void setGetRebalanceTaskSweeper(GetRebalanceTaskSweeper getRebalanceTaskSweeper) {
    this.getRebalanceTaskSweeper = getRebalanceTaskSweeper;
  }

  public void setUserMaxCapacityByte(long userMaxCapacityBytes) {
    this.userMaxCapacityBytes = userMaxCapacityBytes;
  }

  public VolumeRuleRelationshipStore getVolumeRuleRelationshipStore() {
    return volumeRuleRelationshipStore;
  }

  public void setVolumeRuleRelationshipStore(
      VolumeRuleRelationshipStore volumeRuleRelationshipStore) {
    this.volumeRuleRelationshipStore = volumeRuleRelationshipStore;
  }

  public AccessRuleStore getAccessRuleStore() {
    return accessRuleStore;
  }

  public void setAccessRuleStore(AccessRuleStore accessRuleStore) {
    this.accessRuleStore = accessRuleStore;
  }

  public IscsiAccessRuleStore getIscsiAccessRuleStore() {
    return iscsiAccessRuleStore;
  }

  public void setIscsiAccessRuleStore(IscsiAccessRuleStore iscsiAccessRuleStore) {
    this.iscsiAccessRuleStore = iscsiAccessRuleStore;
  }

  public IscsiRuleRelationshipStore getIscsiRuleRelationshipStore() {
    return iscsiRuleRelationshipStore;
  }

  public void setIscsiRuleRelationshipStore(IscsiRuleRelationshipStore iscsiRuleRelationshipStore) {
    this.iscsiRuleRelationshipStore = iscsiRuleRelationshipStore;
  }

  public IoLimitationStore getIoLimitationStore() {
    return ioLimitationStore;
  }

  public void setIoLimitationStore(IoLimitationStore ioLimitationStore) {
    this.ioLimitationStore = ioLimitationStore;
  }

  public MigrationRuleStore getMigrationRuleStore() {
    return migrationRuleStore;
  }

  public void setMigrationRuleStore(MigrationRuleStore migrationRuleStore) {
    this.migrationRuleStore = migrationRuleStore;
  }

  public RebalanceRuleStore getRebalanceRuleStore() {
    return rebalanceRuleStore;
  }

  public void setRebalanceRuleStore(RebalanceRuleStore rebalanceRuleStore) {
    this.rebalanceRuleStore = rebalanceRuleStore;
  }

  public ServerNodeStore getServerNodeStore() {
    return serverNodeStore;
  }

  public void setServerNodeStore(ServerNodeStore serverNodeStore) {
    this.serverNodeStore = serverNodeStore;
  }

  public DiskInfoStore getDiskInfoStore() {
    return diskInfoStore;
  }

  public void setDiskInfoStore(DiskInfoStore diskInfoStore) {
    this.diskInfoStore = diskInfoStore;
  }

  public void setVolumeStatusStore(VolumeStatusTransitionStore volumeStatusStore) {
    this.volumeStatusStore = volumeStatusStore;
  }

  public void setSegmentUnitTimeoutStore(SegmentUnitTimeoutStore segmentUnitTimeoutStore) {
    this.segmentUnitTimeoutStore = segmentUnitTimeoutStore;
  }

  public SelectionStrategy getSelectionStrategy() {
    return selectionStrategy;
  }

  public void setSelectionStrategy(SelectionStrategy selectionStrategy) {
    this.selectionStrategy = selectionStrategy;
  }

  public AppContext getAppContext() {
    return appContext;
  }

  public void setAppContext(InfoCenterAppContext appContext) {
    this.appContext = appContext;
  }

  public OrphanVolumeStore getOrphanVolumes() {
    return orphanVolumes;
  }

  public void setOrphanVolumes(OrphanVolumeStore orphanVolumes) {
    this.orphanVolumes = orphanVolumes;
  }

  public CoordinatorClientFactory getCoordinatorClientFactory() {
    return coordinatorClientFactory;
  }

  public void setCoordinatorClientFactory(CoordinatorClientFactory coordinatorClientFactory) {
    this.coordinatorClientFactory = coordinatorClientFactory;
  }

  public NetworkConfiguration getNetworkConfiguration() {
    return networkConfiguration;
  }

  public void setNetworkConfiguration(NetworkConfiguration networkConfiguration) {
    this.networkConfiguration = networkConfiguration;
  }

  public Map<String, Long> getServerNodeReportTimeMap() {
    return serverNodeReportTimeMap;
  }

  public void setServerNodeReportTimeMap(Map<String, Long> serverNodeReportTimeMap) {
    this.serverNodeReportTimeMap = serverNodeReportTimeMap;
  }

  public LockForSaveVolumeInfo getLockForSaveVolumeInfo() {
    return lockForSaveVolumeInfo;
  }

  public void setLockForSaveVolumeInfo(LockForSaveVolumeInfo lockForSaveVolumeInfo) {
    this.lockForSaveVolumeInfo = lockForSaveVolumeInfo;
  }

  public ReportVolumeManager getReportVolumeManager() {
    return reportVolumeManager;
  }

  public void setReportVolumeManager(ReportVolumeManager reportVolumeManager) {
    this.reportVolumeManager = reportVolumeManager;
  }

  public TaskStore getTaskStore() {
    return taskStore;
  }

  public void setTaskStore(TaskStore taskStore) {
    this.taskStore = taskStore;
  }

  public ScsiClientStore getScsiClientStore() {
    return scsiClientStore;
  }

  public void setScsiClientStore(ScsiClientStore scsiClientStore) {
    this.scsiClientStore = scsiClientStore;
  }

  public ScsiDriverStore getScsiDriverStore() {
    return scsiDriverStore;
  }

  public void setScsiDriverStore(ScsiDriverStore scsiDriverStore) {
    this.scsiDriverStore = scsiDriverStore;
  }

  public long getPageSize() {
    return pageSize;
  }

  public void setPageSize(long pageSize) {
    this.pageSize = pageSize;
  }

  public void setVolumeDelayStore(VolumeDelayStore volumeDelayStore) {
    this.volumeDelayStore = volumeDelayStore;
  }

  public void setVolumeRecycleStore(VolumeRecycleStore volumeRecycleStore) {
    this.volumeRecycleStore = volumeRecycleStore;
  }

  public void setVolumeRecycleManager(VolumeRecycleManager volumeRecycleManager) {
    this.volumeRecycleManager = volumeRecycleManager;
  }

  public void setDriverClientManger(DriverClientManger driverClientManger) {
    this.driverClientManger = driverClientManger;
  }

  public void setServerStatusCheck(ServerStatusCheck serverStatusCheck) {
    this.serverStatusCheck = serverStatusCheck;
  }

  public RecoverDbSentryStore getRecoverDbSentryStore() {
    return recoverDbSentryStore;
  }

  public void setRecoverDbSentryStore(RecoverDbSentryStore recoverDbSentryStore) {
    this.recoverDbSentryStore = recoverDbSentryStore;
  }

  /**
   * enum and class.
   */

  //for check the extend status
  enum VolumeExtendStyle {
    InExtending,
    ExtendFailed,
    Normal
  }

}
