package org.apache.pinot.query.planner.stage;

import java.util.List;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.serde.ProtoProperties;


public class WindowNode extends AbstractStageNode {

  @ProtoProperties
  private List<RexExpression> _expressions;

  public WindowNode(int stageId) {
    super(stageId);
  }
  public WindowNode(int stageId, List<RexExpression> expressions,
      DataSchema dataSchema) {
    super(stageId, dataSchema);
    _expressions = expressions;
  }

  public List<RexExpression> getExpressions() {
    return _expressions;
  }
}
