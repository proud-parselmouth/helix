package org.apache.helix.controller.rebalancer.strategy;

public class CrushEd2RebalanceStrategy extends CrushEdRebalanceStrategy {
  public CrushEd2RebalanceStrategy() {
    _baseStrategy = new CrushRebalanceStrategy(true);
  }
}
