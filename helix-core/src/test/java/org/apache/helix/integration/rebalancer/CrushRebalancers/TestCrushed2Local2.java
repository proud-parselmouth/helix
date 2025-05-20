package org.apache.helix.integration.rebalancer.CrushRebalancers;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.api.config.StateTransitionThrottleConfig;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.rebalancer.strategy.CrushEd2RebalanceStrategy;
import org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.util.TestInputLoader;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestCrushed2Local2 extends ZkTestBase {
  protected static final int START_PORT = 12918;
  private final String NEW_PARTICIPANT_PREFIX = "new_";
  protected final String CLASS_NAME = getShortClassName();
  private final String TOPOLOGY = "/zone/instance";
  private final String FAULT_ZONE_TYPE= "zone";
  private final Integer DELAY_REBALANCE_TIME = 10800000;
  private final Integer PARTICIPANT_ST_DEALY_MS = 20*1000;
  private final Integer MONITOR_DELAY_MS = 20*1000;
  protected final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;
  Map<String, Integer> _instanceToZoneType = new HashMap<>();
  Map<String, String> _instanceToLogicalId = new HashMap<>();
  Map<String, Integer> _newInstanceToZoneType = new HashMap<>();
  protected ClusterControllerManager _controller;
  List<MockParticipantManager> _participants = new ArrayList<>();
  List<String> _nodes = new ArrayList<>();
  List<String> _allDBs = new ArrayList<>();
  ConfigAccessor _configAccessor;
  Map<String, List<String>> newPreferenceList;
  int totalPartitionCount = 0;
  String rebalanceStrategy = "CRUSHED";

  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
    _gSetupTool.addCluster(CLUSTER_NAME, true);
    String controllerName = CONTROLLER_PREFIX + "_0";
    _configAccessor = new ConfigAccessor(_gZkClient);
    updateClusterConfig();
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    createResources(CrushEdRebalanceStrategy.class.getName());
  }

  private Map<String, Integer> getSkewedZoneToInstanceCountMap(char startZoneChar) {
    Map<String, Integer> skewedZoneToInstanceCountMap = new HashMap<>();
    int[] instanceCounts = {1, 8, 8, 8, 8, 8, 8, 17};

    for (int i = 0; i < instanceCounts.length; i++) {
      char zoneChar = (char) (startZoneChar + i);
      skewedZoneToInstanceCountMap.put("zone_" + zoneChar, instanceCounts[i]);
    }

    return skewedZoneToInstanceCountMap;
  }

  private void updateClusterConfig() {
    // your original setup code
    ClusterConfig clusterConfig = new ClusterConfig(CLUSTER_NAME);
    clusterConfig.setTopology(TOPOLOGY);
    clusterConfig.setTopologyAwareEnabled(true);
    clusterConfig.setFaultZoneType(FAULT_ZONE_TYPE);
    clusterConfig.setRebalanceDelayTime(DELAY_REBALANCE_TIME);
    clusterConfig.setPersistBestPossibleAssignment(true);
    List<StateTransitionThrottleConfig> throttleConfigs = new ArrayList<>();
    throttleConfigs.add(new StateTransitionThrottleConfig(StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE,
        StateTransitionThrottleConfig.ThrottleScope.INSTANCE, 150));
    clusterConfig.setStateTransitionThrottleConfigs(throttleConfigs);
    _configAccessor.updateClusterConfig(CLUSTER_NAME, clusterConfig);
  }

  private void createParticipants(String prefix, Map<String, Integer> zoneToInstanceCountMap) {
    int counter = 0;
    for (Map.Entry<String, Integer> entry : zoneToInstanceCountMap.entrySet()) {
      String zone = entry.getKey();
      int instanceCount = entry.getValue();
      String zoneChar = zone.split("_")[1];

      for (int i = 0; i < instanceCount; i++) {
        String participantId = prefix + PARTICIPANT_PREFIX + "_" + zoneChar + "_" + i;
        String participantLogicalId = prefix + "P_" + counter + "_" + instanceCount;
        String participantName = prefix + PARTICIPANT_PREFIX + "_" + (START_PORT + counter);
        _instanceToLogicalId.put(participantName, participantLogicalId);
        _gSetupTool.addInstanceToCluster(CLUSTER_NAME, participantName);
        _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, participantName, true);

        String domain = String.format(
            "zone=%s,instance=%s,applicationInstanceId=%s,host=%s",
            zone, participantId, participantId, participantName
        );

        InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, participantName);
        instanceConfig.setDomain(domain);
        _configAccessor.setInstanceConfig(CLUSTER_NAME, participantName, instanceConfig);

        MockParticipantManager participant = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, participantName, PARTICIPANT_ST_DEALY_MS);
        participant.syncStart();
        _participants.add(participant);

        counter++;
        _instanceToZoneType.put(participantName, instanceCount);

        _nodes.add(participantName);
      }
    }
  }

  private void createResources(String rebalanceStrategy) {
    String[] params =
        new String[]{"id", "NUM_PARTITIONS", "STATE_MODEL_DEF_REF", "REPLICAS", "MIN_ACTIVE_REPLICAS"};
    Object[][] resources = TestInputLoader
        .loadTestInputs("TestCrushed2RebalanceContinuous.Resources.json", params);
    for (Object[] row : resources) {
      String resource = (String) row[0];
      int partitionCount = Integer.parseInt((String) row[1]);
      String stateModel = (String) row[2];
      int replicaCount = Integer.parseInt((String) row[3]);
      int minActiveReplicas = Integer.parseInt((String) row[4]);
      createResourceWithDelayedRebalance(
          CLUSTER_NAME, resource, stateModel, partitionCount, replicaCount, minActiveReplicas, DELAY_REBALANCE_TIME,
          rebalanceStrategy
      );
      totalPartitionCount += partitionCount;
      _allDBs.add(resource);
    }
    System.out.println("Total partitions: " + totalPartitionCount);
  }

  @AfterClass
  public void afterClass() throws Exception {
    for (MockParticipantManager p : _participants) {
      if (p != null && p.isConnected()) {
        p.syncStop();
      }
    }
    if (_controller != null && _controller.isConnected()) {
      _controller.syncStop();
    }
    deleteCluster(CLUSTER_NAME);
    System.out.println("END " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void TestCrushED2RebalanceAssignments() throws InterruptedException {
//    Thread controllerThread = new Thread(this::startController);
//    Thread monitorThread = new Thread(this::monitorUntilEvMatchesIdeal);
//
//    controllerThread.start();
//    monitorThread.start();
//
//    monitorThread.join();
    createParticipants("", getSkewedZoneToInstanceCountMap('A'));
    startController();
    monitorUntilEvMatchesIdeal(false);
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, true, "adding new instances", null);
    updateInstanceWeights();
    createParticipants(NEW_PARTICIPANT_PREFIX, getSkewedZoneToInstanceCountMap('I'));
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, false, "", null);
    Thread.sleep(60 * 1000);
    monitorUntilEvMatchesIdeal(true);
  }

  private void updateInstanceWeights() {
    for (String instance : _nodes) {
      InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, instance);
      instanceConfig.setWeight(0);
      _configAccessor.setInstanceConfig(CLUSTER_NAME, instance, instanceConfig);
    }
  }

  private void startController() {
    _controller.syncStart();
  }

  private void monitorUntilEvMatchesIdeal(boolean flag) {
    long totalEvTimeNs = 0;
    long totalIsTimeNs = 0;
    int iterations = 0;

    String fileName = "target/instancePartitionStats-" + rebalanceStrategy + "_" + flag + ".csv";
    File file = new File(fileName);

    try (PrintWriter writer = new PrintWriter(new FileWriter(file, false))) {
      boolean headerWritten = false;

      while (true) {
        long startEv = System.nanoTime();
        Map<String, ExternalView> ev = getEVsFromCurrentState();
        long endEv = System.nanoTime();
        totalEvTimeNs += (endEv - startEv);

        long startIs = System.nanoTime();
        Map<String, IdealState> is = getIS();
        long endIs = System.nanoTime();
        totalIsTimeNs += (endIs - startIs);

        iterations++;

        if (isIsEmpty(is)) {
          System.out.println("Ideal state is empty at " + new Date());
          continue;
        }
        if (isEvEmpty(ev)) {
          System.out.println("Ev is empty at " + new Date());
          continue;
        }
        Map<String, ZNRecord> assignments = getEvZnRecords(ev);
        if (flag) {
          boolean hasNewInstance = assignments.values()
              .stream()
              .anyMatch(zn -> zn.getMapFields()
                  .values()
                  .stream()
                  .anyMatch(
                      map -> map.keySet().stream().anyMatch(instance -> instance.startsWith(NEW_PARTICIPANT_PREFIX))));
          if (!hasNewInstance) {
            continue;
          }
        }
        printAssignments(assignments, "external view", writer, !headerWritten, flag);
        headerWritten = true;

        if (idealStateEVMatch(is, ev, flag)) {
          System.out.println("ExternalView matches IdealState at " + new Date());
          break;
        }

        try {
          Thread.sleep(MONITOR_DELAY_MS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          System.err.println("Monitor thread interrupted.");
          break;
        }
      }

      double meanEvMs = totalEvTimeNs / 1_000_000.0 / iterations;
      double meanIsMs = totalIsTimeNs / 1_000_000.0 / iterations;
      System.out.printf("Mean time over %d iterations: getEVs = %.2f ms, getIS = %.2f ms%n", iterations, meanEvMs, meanIsMs);

    } catch (IOException e) {
      e.printStackTrace();
    }
  }


  private Map<String, ZNRecord> getEvZnRecords(Map<String, ExternalView> evMap) {
    Map<String, ZNRecord> znRecords = new HashMap<>();
    for (Map.Entry<String, ExternalView> entry : evMap.entrySet()) {
      znRecords.put(entry.getKey(), entry.getValue().getRecord());
    }
    return znRecords;
  }

  private Map<String, ZNRecord> getIsnRecords(Map<String, IdealState> isMap) {
    Map<String, ZNRecord> znRecords = new HashMap<>();
    for (Map.Entry<String, IdealState> entry : isMap.entrySet()) {
      znRecords.put(entry.getKey(), entry.getValue().getRecord());
    }
    return znRecords;
  }

  private boolean isIsEmpty(Map<String, IdealState> ismap) {
    for (Map.Entry<String, IdealState> entry : ismap.entrySet()) {
      IdealState is = entry.getValue();
      if(!is.getRecord().getMapFields().isEmpty()) {
        return false;
      }
    }
    return true;
  }

  private Map<String, ExternalView> getEVs() {
    Map<String, ExternalView> externalViews = new HashMap<String, ExternalView>();
    for (String db : _allDBs) {
      ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      externalViews.put(db, ev);
    }
    return externalViews;
  }

  private boolean isEvEmpty(Map<String, ExternalView> evmap) {
    for (Map.Entry<String, ExternalView> entry : evmap.entrySet()) {
      ExternalView ev = entry.getValue();
      if(!ev.getRecord().getMapFields().isEmpty()) {
        return false;
      }
    }
    return true;
  }

  private void printAssignments(Map<String, ZNRecord> assignments,
      String recordType,
      PrintWriter writer,
      boolean writeHeader,
      boolean flag) {
    Map<String, List<String>> mergedPreferenceList = new HashMap<>();

    for (String resource : _allDBs) {
      Map<String, List<String>> preferenceList = extractPreferenceList(assignments.get(resource));
      preferenceList.forEach((partition, instances) -> {
        mergedPreferenceList.put(partition, instances);
      });
    }


    Date d = new Date(System.currentTimeMillis());
    printInstancePartitionStats(mergedPreferenceList, d, _instanceToZoneType, writer, writeHeader);

    System.out.println("Printing " + recordType + " assignments");
    printMaxPartitionsByZoneType(mergedPreferenceList, d, _instanceToZoneType);
  }

  private void printInstancePartitionStats(Map<String, List<String>> mergedPreferenceList,
      Date d,
      Map<String, Integer> instanceToZoneTypeMap,
      PrintWriter writer,
      boolean writeHeader) {
    Map<String, List<Double>> instancePartitionStats =
        CrushED2TestUtils.computeInstancePartitionStats(mergedPreferenceList, instanceToZoneTypeMap);


    // Build headers from instance names with zoneType
    Map<String, String> instanceHeaderMap = new TreeMap<>();
    for (String instance : _instanceToZoneType.keySet()) {
      int zoneType = _instanceToZoneType.get(instance);
      instanceHeaderMap.put(instance, _instanceToLogicalId.get(instance)) ;
    }

    // Extract common stats
    int totalPartitions = instancePartitionStats.values().stream()
        .mapToInt(stats -> stats.get(4).intValue())
        .findFirst().orElse(0);
    double globalAvg = instancePartitionStats.values().stream()
        .mapToDouble(stats -> stats.get(2))
        .findFirst().orElse(0);

    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    String timestamp = formatter.format(d);

    // Header
    if (writeHeader) {
      writer.print("timestamp,totalPartitionCount,globalAvg");
      for (String header : instanceHeaderMap.values()) {
        writer.print("," + header);
      }
      writer.println();
    }

    // Row
    writer.printf("%s,%d,%.2f", timestamp, totalPartitions, globalAvg);
    for (String instance : instanceHeaderMap.keySet()) {
      double partitionCount = 0.0;
      if (instancePartitionStats.containsKey(instance)) {
        partitionCount = instancePartitionStats.get(instance).get(1);
      }
      writer.printf(",%.0f", partitionCount);
    }
    writer.println();
    writer.flush();
  }

  private void printMaxPartitionsByZoneType(Map<String, List<String>> mergedPreferenceList, Date d, Map<String, Integer> instanceToZoneType) {
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    String timestamp = formatter.format(d);
    Map<Integer, List<Double>> maxPartitionsByZoneType =
        CrushED2TestUtils.getMaxPartitionsPerZoneType(mergedPreferenceList, instanceToZoneType);

    // Define fixed column widths
    int colWidth = 28;

    // Build headers
    StringBuilder header = new StringBuilder();
    header.append(String.format("%-" + colWidth + "s", "Timestamp"));
    header.append(String.format("%-" + colWidth + "s", "Total Partition Count"));
    header.append(String.format("%-" + colWidth + "s", "Global Avg"));

    // For consistent ordering
    List<Integer> sortedKeys = new ArrayList<>(maxPartitionsByZoneType.keySet());
    Collections.sort(sortedKeys);

    for (int zone : sortedKeys) {
      header.append(String.format("%-" + colWidth + "s", "Max(mz=" + zone + ")"));
      header.append(String.format("%-" + colWidth + "s", "Avg(mz=" + zone + ")"));
      header.append(String.format("%-" + colWidth + "s", "Skew(mz=" + zone + ")"));
    }

    // Print header
    System.out.println(header);

    // Build values row
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("%-" + colWidth + "s", timestamp));

    if (!maxPartitionsByZoneType.isEmpty()) {
      double globalAvg = maxPartitionsByZoneType.values().iterator().next().get(3);
      double totalPartitionCount = maxPartitionsByZoneType.values().iterator().next().get(4);
      sb.append(String.format("%-" + colWidth + "s", String.format("%.1f", totalPartitionCount)));
      sb.append(String.format("%-" + colWidth + "s", String.format("%.1f", globalAvg)));

      for (int zone : sortedKeys) {
        List<Double> values = maxPartitionsByZoneType.get(zone);
        sb.append(String.format("%-" + colWidth + "s", String.format("%.1f", values.get(0))));
        sb.append(String.format("%-" + colWidth + "s", String.format("%.1f", values.get(1))));
        sb.append(String.format("%-" + colWidth + "s", String.format("%.2f", values.get(2))));
      }
    }

    System.out.println(sb);
  }



  private Map<String, List<String>> extractPreferenceList(ZNRecord record) {
    Map<String, List<String>> preferenceLists = new HashMap<>();
    Map<String, Map<String, String>> mapFields = record.getMapFields();

    if (mapFields == null || mapFields.isEmpty()) {
      return preferenceLists; // return empty map
    }

    mapFields.forEach((partition, instanceStateMap) -> {
      List<String> instances = new ArrayList<>(instanceStateMap.keySet());
      preferenceLists.put(partition, instances);
    });

    return preferenceLists;
  }

  private Map<String, ExternalView> getEVsFromCurrentState() {
    ExecutorService executor = Executors.newFixedThreadPool(  100);
    Map<String, Future<ExternalView>> futureMap = new HashMap<>();

    for (String db : _allDBs) {
      futureMap.put(db, executor.submit(() ->
          _gSetupTool.getClusterManagementTool().getResourceEVFromCurrentState(CLUSTER_NAME, db, _nodes)
      ));
    }

    Map<String, ExternalView> externalViews = new HashMap<>();
    for (Map.Entry<String, Future<ExternalView>> entry : futureMap.entrySet()) {
      try {
        externalViews.put(entry.getKey(), entry.getValue().get());
      } catch (Exception e) {
        System.err.println("Failed to fetch EV from current state for DB " + entry.getKey() + ": " + e.getMessage());
      }
    }

    executor.shutdown();
    return externalViews;
  }

  private Map<String, IdealState> getIS() {
    ExecutorService executor = Executors.newFixedThreadPool(100);
    Map<String, Future<IdealState>> futureMap = new HashMap<>();

    for (String db : _allDBs) {
      futureMap.put(db, executor.submit(() ->
          _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db)
      ));
    }

    Map<String, IdealState> idealStates = new HashMap<>();
    for (Map.Entry<String, Future<IdealState>> entry : futureMap.entrySet()) {
      try {
        idealStates.put(entry.getKey(), entry.getValue().get());
      } catch (Exception e) {
        System.err.println("Failed to fetch IS for DB " + entry.getKey() + ": " + e.getMessage());
      }
    }

    executor.shutdown();
    return idealStates;
  }

  private boolean idealStateEVMatch(Map<String, IdealState> isMap, Map<String, ExternalView> evMap, boolean flag) {
    if (flag) {
      boolean hasNewInstance = isMap.values().stream()
          .flatMap(is -> is.getRecord().getListFields().values().stream())
          .flatMap(List::stream)
          .anyMatch(instance -> instance.startsWith(NEW_PARTICIPANT_PREFIX));
      if (!hasNewInstance) {
        return false;
      }
    }
    if (getEvPartitionCount(evMap) != totalPartitionCount) {
      return false;
    }
    for (String resource : isMap.keySet()) {
      ExternalView ev = evMap.get(resource);
      IdealState ideal = isMap.get(resource);
      Map<String, Map<String, String>> evMapFields = ev.getRecord().getMapFields();
      Map<String, Map<String, String>> isMapFields = ideal.getRecord().getMapFields();

      for (String partition : isMapFields.keySet()) {
        Map<String, String> idealInstanceMap = isMapFields.get(partition);
        Map<String, String> evInstanceMap = evMapFields.get(partition);

        if (!idealInstanceMap.equals(evInstanceMap)) {
          return false;
        }
      }
    }
//    System.out.println("UnmatchedPartions Count " + unmatchedPartions.size());
//    System.out.println("UnmatchedPartions:" + unmatchedPartions);
    return true;
  }

  private int getEvPartitionCount(Map<String, ExternalView> evMap) {
    int partitionCount = 0;
    for (String resource : evMap.keySet()) {
      ExternalView ev = evMap.get(resource);
      partitionCount += ev.getPartitionSet().size();
    }
    return partitionCount;
  }

}
