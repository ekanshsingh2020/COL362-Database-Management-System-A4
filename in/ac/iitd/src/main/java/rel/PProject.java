package rel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import convention.PConvention;

import java.util.List;
import java.util.ArrayList;
import java.util.function.BinaryOperator;
// Hint: Think about alias and arithmetic operations
public class PProject extends Project implements PRel {

    public PProject(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            List<? extends RexNode> projects,
            RelDataType rowType) {
        super(cluster, traits, ImmutableList.of(), input, projects, rowType);
        assert getConvention() instanceof PConvention;
    }

    @Override
    public PProject copy(RelTraitSet traitSet, RelNode input,
                            List<RexNode> projects, RelDataType rowType) {
        return new PProject(getCluster(), traitSet, input, projects, rowType);
    }

    @Override
    public String toString() {
        return "PProject";
    }


    private static Object evaluateRexCall(Object [] record, RexCall rexCall) {
        if (rexCall == null) {
            return null;
        }

        SqlOperator operator = rexCall.getOperator();
        List<RexNode> operands = rexCall.getOperands();

        if (operator == SqlStdOperatorTable.PLUS) {
            // Handle addition
            return evaluateNaryOperation(record, operands, Double::sum);
        } else if (operator == SqlStdOperatorTable.MINUS) {
            // Handle subtraction
            return evaluateNaryOperation(record, operands, (a, b) -> a - b);
        } else if (operator == SqlStdOperatorTable.MULTIPLY) {
            // Handle multiplication
            return evaluateNaryOperation(record, operands, (a, b) -> a * b);
        } else if (operator == SqlStdOperatorTable.DIVIDE) {
            // Handle division
            return evaluateNaryOperation(record, operands, (a, b) -> a / b);
        } else {
            return null;
        }
    }

    public static Object evaluateNaryOperation(Object[] record, List<RexNode> operands, BinaryOperator<Double> operation) {
        if (operands.isEmpty()) {
            return null;
        }

        List<Double> values = new ArrayList<>();

        for (int i = 0; i < operands.size(); i++) {
            Number val = (Number) evaluateRexNode(record, operands.get(i));
            if(val == null){
                return null;
            }
            values.add(val.doubleValue());
        }

        Object result = values.get(0);
        // Perform the operation with the remaining operands
        for (int i = 1; i < operands.size(); i++) {
            result = operation.apply((double)result, values.get(i));
        }

        return result;
    }

    public static Object evaluateRexNode(Object[] record, RexNode rexNode) {
        if (rexNode instanceof RexCall) {
            return evaluateRexCall(record, (RexCall) rexNode);
        } else if (rexNode instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) rexNode;
            return literal.getValue();
        } else if (rexNode instanceof RexInputRef) {
            return record[((RexInputRef) rexNode).getIndex()];
        }
        else {
            return null; 
        }
    }

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open(){
        logger.trace("Opening PProject");
        System.out.println("PProject opened");
        // System.out.println(this.input);
        // System.out.println("Projects of PProject "+this.getProjects());
        if (this.input instanceof PRel) {
            PRel inputNode = (PRel) this.input;
            return inputNode.open();
        }
        return false;
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PProject");
        System.out.println("Closing PProject");
        if (this.input instanceof PRel) {
            PRel inputNode = (PRel) this.input;
            inputNode.close();
        }
        return;
    }

    // returns true if there is a next row, false otherwise

    Object [] temp = null;

    @Override
    public boolean hasNext(){
        logger.trace("Checking if PProject has next");
        if (this.input instanceof PRel) {
            PRel inputNode = (PRel) this.input;
            while(inputNode.hasNext())
            {
                temp = inputNode.next();
                if(temp!=null)
                    return true;
            }
            System.out.println("No more rows in PProject");
            return false;
        }
        return false;
    }

    // returns the next row


    @Override
    public Object[] next(){
        logger.trace("Getting next row from PProject");
        if (this.input instanceof PRel) {
            List<RexNode> projects = (List<RexNode>)this.getProjects();
            Object [] result = new Object[projects.size()];


            for (int i = 0; i < projects.size(); i++) {
                RexNode exp = projects.get(i);
                if (exp instanceof RexInputRef) {
                    result[i] = temp[((RexInputRef) exp).getIndex()];
                } else if (exp instanceof RexLiteral) {
                    RexLiteral literal = (RexLiteral) exp;
                    result[i] = literal.getValue();
                } else if (exp instanceof RexCall) {
                    result[i] = evaluateRexCall(temp, (RexCall) exp);
                }
            }

            return result;
        }
        return null;
    }
}