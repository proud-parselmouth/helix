package org.apache.helix.integration.rebalancer.CrushRebalancers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class CrushED2TestUtils {
  public static Map<Integer, List<Double>> getAvgPartitionsPerZoneType(
      Map<String, List<String>> preferenceList,
      Map<String, Integer> instanceToZoneType
  ) {
    Map<String, Integer> instancePartitionCount = countPartitionsPerInstance(preferenceList);
    Map<Integer, List<Integer>> zoneTypeToPartitionCounts =
        groupPartitionCountsByZone(instancePartitionCount, instanceToZoneType);
    return computeAveragePerZoneType(zoneTypeToPartitionCounts);
  }

  public static Map<Integer, List<Double>> getMaxPartitionsPerZoneType(
      Map<String, List<String>> preferenceList,
      Map<String, Integer> instanceToZoneType
  ) {
    Map<String, Integer> instancePartitionCount = countPartitionsPerInstance(preferenceList);
    Map<Integer, List<Integer>> zoneTypeToPartitionCounts =
        groupPartitionCountsByZone(instancePartitionCount, instanceToZoneType);
    return computeMaxPerZoneType(zoneTypeToPartitionCounts);
  }

  private static Map<String, Integer> countPartitionsPerInstance(Map<String, List<String>> preferenceList) {
    Map<String, Integer> instancePartitionCount = new HashMap<>();
    preferenceList.values().forEach(instances ->
        instances.forEach(instance ->
            instancePartitionCount.merge(instance, 1, Integer::sum)
        )
    );
    return instancePartitionCount;
  }

  private static Map<Integer, List<Integer>> groupPartitionCountsByZone(
      Map<String, Integer> instancePartitionCount,
      Map<String, Integer> instanceToZoneType
  ) {
    Map<Integer, List<Integer>> zoneTypeToPartitionCounts = new HashMap<>();
    instanceToZoneType.forEach((instance, zoneType) -> {
      int count = instancePartitionCount.getOrDefault(instance, 0);
      zoneTypeToPartitionCounts
          .computeIfAbsent(zoneType, z -> new ArrayList<>())
          .add(count);
    });
    return zoneTypeToPartitionCounts;
  }

  private static Map<Integer, List<Double>> computeAveragePerZoneType(Map<Integer, List<Integer>> zoneToCounts) {
    Map<Integer, List<Double>> avgPerZone = new HashMap<>();
    final double[] totalPartitions = {0.0};
    final int[] totalInstances = {0};

    // Pass 1: calculate average per zone + accumulate global sums
    zoneToCounts.forEach((zoneType, counts) -> {
      double sum = counts.stream().mapToInt(i -> i).sum();
      int instanceCount = counts.size();
      double avg = instanceCount != 0 ? sum / instanceCount : 0.0;

      avgPerZone.put(zoneType, new ArrayList<>(List.of(avg)));  // store only avg for now

      totalPartitions[0] += sum;
      totalInstances[0] += instanceCount;
    });

    // Compute ideal (global) average
    double idealAvg = totalInstances[0] != 0 ? totalPartitions[0] / totalInstances[0] : 1.0;

    // Pass 2: calculate skew vs. ideal and add to avgPerZone
    avgPerZone.forEach((zoneType, list) -> {
      double avg = list.get(0);
      double skew = idealAvg != 0.0 ? avg / idealAvg : Double.POSITIVE_INFINITY;
      list.add(skew);
      list.add(idealAvg);
    });

    return avgPerZone;
  }

  private static Map<Integer, List<Double>> computeMaxPerZoneType(Map<Integer, List<Integer>> zoneToCounts) {
    Map<Integer, List<Double>> maxPerZone = new HashMap<>();
    final double[] totalPartitions = {0.0};
    final int[] totalInstances = {0};

    // Pass 1: calculate average per zone + accumulate global sums
    zoneToCounts.forEach((zoneType, counts) -> {
      double sum = counts.stream().mapToInt(i -> i).sum();
      double max = counts.stream().mapToInt(i -> i).max().orElse(0);
      int instanceCount = counts.size();
      double avg = instanceCount != 0 ? sum / instanceCount : 0.0;

      maxPerZone.put(zoneType, new ArrayList<>(List.of(max, avg)));  // store max and avg

      totalPartitions[0] += sum;
      totalInstances[0] += instanceCount;
    });

    // Compute ideal (global) average
    double idealAvg = totalInstances[0] != 0 ? Math.round(((totalPartitions[0] / totalInstances[0]) * 100)) / 100 : 1.0;

    // Pass 2: calculate skew vs. ideal and add to avgPerZone
    maxPerZone.forEach((zoneType, list) -> {
      double max = list.get(0);
      double skew = idealAvg != 0.0 ? max / idealAvg : 0.0;
      skew = Math.round(skew * 100.0) / 100.0; // round to 2 decimal places
      list.add(skew);
      list.add(idealAvg);
      list.add(totalPartitions[0]);
    });

    return maxPerZone;
  }

  public static Map<String, List<Double>> computeInstancePartitionStats(
      Map<String, List<String>> preferenceList,
      Map<String, Integer> instanceToZoneType
  ) {
    // Count how many partitions each instance appears in
    Map<String, Integer> partitionCounts = new HashMap<>();
    preferenceList.values().forEach(instances ->
        instances.forEach(instance ->
            partitionCounts.merge(instance, 1, Integer::sum)
        )
    );

    // Compute global average
    double totalPartitions = partitionCounts.values().stream().mapToInt(Integer::intValue).sum();
    int totalInstances = instanceToZoneType.size();
    double avg = totalInstances > 0 ? totalPartitions / totalInstances : 1.0;

    // Build final map
    Map<String, List<Double>> stats = new HashMap<>();
    partitionCounts.forEach((instance, count) -> {
      int zoneType = instanceToZoneType.getOrDefault(instance, -1);
      double skew = avg > 0 ? (double) count / avg : 0.0;
      skew = Math.round(skew * 100.0) / 100.0; // round to 2 decimal places

        stats.put(instance, List.of(
            (double) zoneType,
            (double) count,
            avg,
            skew,
            totalPartitions
        ));
    });

    return stats;
  }
}
