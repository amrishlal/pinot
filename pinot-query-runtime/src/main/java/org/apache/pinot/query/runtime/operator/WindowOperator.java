package org.apache.pinot.query.runtime.operator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;


public class WindowOperator extends BaseOperator<TransferableBlock> {
  private static final String EXPLAIN_NAME = "WINDOW_FUNCTION";
  private final BaseOperator<TransferableBlock> _upstreamOperator;
  private final DataSchema _dataSchema;
  private TransferableBlock _upstreamErrorBlock;
  public WindowOperator(BaseOperator<TransferableBlock> upstreamOperator, List<RexExpression> expressions,
      DataSchema dataSchema) {
    _upstreamOperator = upstreamOperator;
    _dataSchema = dataSchema;
  }

  @Override
  public List<Operator> getChildOperators() {
    return null;
  }

  @Nullable
  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected TransferableBlock getNextBlock() {
    return _upstreamOperator.nextBlock();
  }

  private List<OrderByExpressionContext> toOrderByExpressions(List<RexExpression> collationKeys,
      List<RelFieldCollation.Direction> collationDirections) {
    List<OrderByExpressionContext> orderByExpressionContextList = new ArrayList<>(collationKeys.size());
    for (int i = 0; i < collationKeys.size(); i++) {
      orderByExpressionContextList.add(new OrderByExpressionContext(ExpressionContext.forIdentifier(
          _dataSchema.getColumnName(((RexExpression.InputRef) collationKeys.get(i)).getIndex())),
          !collationDirections.get(i).isDescending()));
    }
    return orderByExpressionContextList;
  }
}
