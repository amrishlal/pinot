package org.apache.pinot.core.query.aggregation;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.selection.WindowFunctionContext;
import org.apache.pinot.segment.local.customobject.window.WindowAccumulator;


/** This class is used to carry out aggregation during window function evaluation. */
public class WindowAggregationExecutor implements AggregationExecutor<WindowAccumulator> {
  private final WindowFunctionContext _windowFunctionContext;
  private final List<AggregationResultHolder> _aggregationResultHolders;
  private final List<WindowAccumulator> _aggregationResults;

  public WindowAggregationExecutor(WindowFunctionContext windowFunctionContext) {
    _windowFunctionContext = windowFunctionContext;
    _aggregationResultHolders = new ArrayList<>(_windowFunctionContext.getNumWindowFunctions());
    _aggregationResults = new ArrayList<>(_windowFunctionContext.getNumWindowFunctions());
    for (int i = 0; i < _windowFunctionContext.getNumWindowFunctions(); i++) {
      _aggregationResultHolders.add(
          _windowFunctionContext.getAggregationFunctions().get(i).createAggregationResultHolder());
      _aggregationResults.add(WindowAccumulatorFactory.create(_windowFunctionContext.getAggregationFunctionClass(i)));
    }
  }

  @Override
  public void aggregate(TransformBlock transformBlock) {
    int length = transformBlock.getNumDocs();
    for (int i = 0; i < _windowFunctionContext.getNumWindowFunctions(); i++) {
      AggregationFunction aggregationFunction = _windowFunctionContext.getAggregationFunctions().get(i);
      aggregationFunction.aggregate(length, _aggregationResultHolders.get(i),
          AggregationFunctionUtils.getBlockValSetMap(aggregationFunction, transformBlock));
    }
  }

  @Override
  public List<WindowAccumulator> getResult() {
    for (int i = 0; i < _windowFunctionContext.getNumWindowFunctions(); i++) {
      _aggregationResults.get(i).apply(_windowFunctionContext.getAggregationFunctions().get(i)
          .extractAggregationResult(_aggregationResultHolders.get(i)));
    }
    return _aggregationResults;
  }
}
