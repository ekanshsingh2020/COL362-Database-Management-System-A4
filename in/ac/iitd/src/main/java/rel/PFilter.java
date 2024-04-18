package rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.rex.RexCall;

import convention.PConvention;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexInputRef;

import java.math.BigDecimal;
import java.util.*;
import java.util.function.BinaryOperator;


public class PFilter extends Filter implements PRel {

    public PFilter(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode child,
            RexNode condition) {
        super(cluster, traits, child, condition);
        assert getConvention() instanceof PConvention;
    }

    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new PFilter(getCluster(), traitSet, input, condition);
    }

    @Override
    public String toString() {
        return "PFilter";
    }

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open(){
        logger.trace("Opening PFilter");
        System.out.println("PFilter opened");
        // System.out.println(this.getCondition());
        if (this.input instanceof PRel) {
            PRel inputNode = (PRel) this.input;
            return inputNode.open();
        }
        return false;
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PFilter");
        System.out.println("PFilter closed");
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
        logger.trace("Checking if PFilter has next");

        if (this.input instanceof PRel) {
            PRel inputNode = (PRel) this.input;
            while(inputNode.hasNext())
            {
                Object [] tempo = inputNode.next();
                EvaluateFilter eval = new EvaluateFilter(true, tempo);
                if(getCondition().accept(eval)) 
                {
                    temp = tempo;
                    return true;
                }
            }
            System.out.println("No more rows in PFilter");
            return false;
        }
        return false;
    }

    // returns the next row
    // Hint: Try looking at different possible filter conditions
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PFilter");
        // System.out.println("Getting next row from PFilter");
        if (this.input instanceof PRel) {
            return temp;
        }
        return null;
    }

    private Object evaluateRexCall(Object [] record, RexCall rexCall) 
        {
            if (rexCall == null) {
                return null;
            }

            // Get the operator and operands
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

        public Object evaluateNaryOperation(Object[] record, List<RexNode> operands, BinaryOperator<Double> operation) 
        {
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

        public Object evaluateRexNode(Object[] record, RexNode rexNode) 
        {
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


    private static final EnumSet<SqlKind> SUPPORTED_OPS =
            EnumSet.of(SqlKind.AND, SqlKind.OR,
                    SqlKind.EQUALS, SqlKind.NOT_EQUALS,
                    SqlKind.LESS_THAN, SqlKind.GREATER_THAN,
                    SqlKind.GREATER_THAN_OR_EQUAL, SqlKind.LESS_THAN_OR_EQUAL,SqlKind.IS_NULL,SqlKind.IS_NOT_NULL);

    private class EvaluateFilter extends RexVisitorImpl<Boolean> 
    {

        final Object[] record;
        protected EvaluateFilter(boolean deep, Object[] record) 
        {
            super(deep);
            this.record = record;
        }

        @Override
        public Boolean visitCall(RexCall call) 
        {
            SqlKind kind = call.getKind();
            if(!kind.belongsTo(SUPPORTED_OPS)) 
            {
                throw new IllegalStateException("Cannot handle this filter predicate yet");
            }

            if(kind == SqlKind.AND) 
            {
                boolean accept = true;
                for(RexNode operand : call.getOperands()) 
                {
                    accept = accept && operand.accept(this);
                }
                return accept;
            } 
            else if(kind == SqlKind.OR) 
            {
                boolean accept = false;
                for(RexNode operand : call.getOperands()) 
                {
                    accept = accept || operand.accept(this);
                }
                return accept;
            }
            else if(kind == SqlKind.IS_NULL)
            {
                RexInputRef rexInputRef = (RexInputRef)call.getOperands().get(0);
                int index = rexInputRef.getIndex();
                Object field = record[index];
                if(field == null) return true;
                return false;
            }
            else if(kind == SqlKind.IS_NOT_NULL)
            {
                RexInputRef rexInputRef = (RexInputRef)call.getOperands().get(0);
                int index = rexInputRef.getIndex();
                Object field = record[index];
                if(field == null) return false;
                return true;
            }
            else 
            {
                return eval(record, kind, call.getOperands().get(0), call.getOperands().get(1));
            }
        }

        public boolean eval(Object[] record, SqlKind kind, RexNode leftOperand, RexNode rightOperand) 
        {

            if(leftOperand instanceof RexInputRef && rightOperand instanceof RexLiteral) 
            {
                RexInputRef rexInputRef = (RexInputRef)leftOperand;
                int index = rexInputRef.getIndex();
                Object field = record[index];
                if(field == null) return false; // basic NULL Handling
                RexLiteral rexLiteral = (RexLiteral) rightOperand;
                switch (kind) 
                {
                    case GREATER_THAN:
                        return isGreaterThan(field, rexLiteral);
                    case LESS_THAN:
                        return isLessThan(field, rexLiteral);
                    case EQUALS:
                        return isEqualTo(field, rexLiteral);
                    case NOT_EQUALS:
                        return !isEqualTo(field, rexLiteral);
                    case GREATER_THAN_OR_EQUAL:
                        return isGreaterThan(field, rexLiteral) || isEqualTo(field, rexLiteral);
                    case LESS_THAN_OR_EQUAL:
                        return isLessThan(field, rexLiteral) || isEqualTo(field, rexLiteral);
                    default:
                        throw new IllegalStateException("Predicate not supported yet");
                }

            }
            else if(leftOperand instanceof RexInputRef && rightOperand instanceof RexInputRef)
            {
                RexInputRef rexInputRef1 = (RexInputRef)leftOperand;
                int index1 = rexInputRef1.getIndex();
                Object field1 = record[index1];
                if(field1 == null) return false; // basic NULL Handling
                RexInputRef rexInputRef2 = (RexInputRef)rightOperand;
                int index2 = rexInputRef2.getIndex();
                Object field2 = record[index2];
                if(field2 == null) return false; // basic NULL Handling
                switch (kind) 
                {
                    case GREATER_THAN:
                        return isGreaterThanNew(field1, field2);
                    case LESS_THAN:
                        return isLessThanNew(field1, field2);
                    case EQUALS:
                        return isEqualToNew(field1, field2);
                    case NOT_EQUALS:
                        return !isEqualToNew(field1, field2);
                    case GREATER_THAN_OR_EQUAL:
                        return isGreaterThanNew(field1, field2) || isEqualToNew(field1, field2);
                    case LESS_THAN_OR_EQUAL:
                        return isLessThanNew(field1, field2) || isEqualToNew(field1, field2);
                    default:
                        throw new IllegalStateException("Predicate not supported yet");
                }
            }
            else if(leftOperand instanceof RexCall && rightOperand instanceof RexLiteral)
            {
                Object tempobj = evaluateRexCall(record,(RexCall)leftOperand);
                if(tempobj == null) return false; // basic NULL Handling
                RexLiteral rexLiteral = (RexLiteral) rightOperand;
                switch (kind) 
                {
                    case GREATER_THAN:
                        return isGreaterThan(tempobj, rexLiteral);
                    case LESS_THAN:
                        return isLessThan(tempobj, rexLiteral);
                    case EQUALS:
                        return isEqualTo(tempobj, rexLiteral);
                    case NOT_EQUALS:
                        return !isEqualTo(tempobj, rexLiteral);
                    case GREATER_THAN_OR_EQUAL:
                        return isGreaterThan(tempobj, rexLiteral) || isEqualTo(tempobj, rexLiteral);
                    case LESS_THAN_OR_EQUAL:
                        return isLessThan(tempobj, rexLiteral) || isEqualTo(tempobj, rexLiteral);
                    default:
                        throw new IllegalStateException("Predicate not supported yet");
                }
            } 
            else if(leftOperand instanceof RexCall && rightOperand instanceof RexInputRef)
            {
                Object tempobj = evaluateRexCall(record,(RexCall)leftOperand);
                if(tempobj == null) return false; // basic NULL Handling
                RexInputRef rexInputRef = (RexInputRef)rightOperand;
                int index = rexInputRef.getIndex();
                Object field = record[index];
                if(field == null) return false; // basic NULL Handling
                switch (kind) 
                {
                    case GREATER_THAN:
                        return isGreaterThanNew(tempobj, field);
                    case LESS_THAN:
                        return isLessThanNew(tempobj, field);
                    case EQUALS:
                        return isEqualToNew(tempobj, field);
                    case NOT_EQUALS:
                        return !isEqualToNew(tempobj, field);
                    case GREATER_THAN_OR_EQUAL:
                        return isGreaterThanNew(tempobj, field) || isEqualToNew(tempobj, field);
                    case LESS_THAN_OR_EQUAL:
                        return isLessThanNew(tempobj, field) || isEqualToNew(tempobj, field);
                    default:
                        throw new IllegalStateException("Predicate not supported yet");
                }
            }
            else if(leftOperand instanceof RexInputRef && rightOperand instanceof RexCall)
            {
                RexInputRef rexInputRef = (RexInputRef)leftOperand;
                int index = rexInputRef.getIndex();
                Object field = record[index];
                if(field == null) return false; // basic NULL Handling
                Object tempobj = evaluateRexCall(record,(RexCall)rightOperand);
                if(tempobj == null) return false; // basic NULL Handling
                switch (kind) 
                {
                    case GREATER_THAN:
                        return isGreaterThanNew(field, tempobj);
                    case LESS_THAN:
                        return isLessThanNew(field, tempobj);
                    case EQUALS:
                        return isEqualToNew(field, tempobj);
                    case NOT_EQUALS:
                        return !isEqualToNew(field, tempobj);
                    case GREATER_THAN_OR_EQUAL:
                        return isGreaterThanNew(field, tempobj) || isEqualToNew(field, tempobj);
                    case LESS_THAN_OR_EQUAL:
                        return isLessThanNew(field, tempobj) || isEqualToNew(field, tempobj);
                    default:
                        throw new IllegalStateException("Predicate not supported yet");
                }
            }
            else if(leftOperand instanceof RexLiteral && rightOperand instanceof RexCall)
            {
                RexLiteral rexLiteral = (RexLiteral) leftOperand;
                Object tempobj = evaluateRexCall(record,(RexCall)rightOperand);
                if(tempobj == null) return false; // basic NULL Handling
                switch (kind) 
                {
                    case GREATER_THAN:
                        return isGreaterThan(tempobj, rexLiteral);
                    case LESS_THAN:
                        return isLessThan(tempobj, rexLiteral);
                    case EQUALS:
                        return isEqualTo(tempobj, rexLiteral);
                    case NOT_EQUALS:
                        return !isEqualTo(tempobj, rexLiteral);
                    case GREATER_THAN_OR_EQUAL:
                        return isGreaterThan(tempobj, rexLiteral) || isEqualTo(tempobj, rexLiteral);
                    case LESS_THAN_OR_EQUAL:
                        return isLessThan(tempobj, rexLiteral) || isEqualTo(tempobj, rexLiteral);
                    default:
                        throw new IllegalStateException("Predicate not supported yet");
                }
            }
            else if(leftOperand instanceof RexCall && rightOperand instanceof RexCall)
            {
                Object tempobj1 = evaluateRexCall(record,(RexCall)leftOperand);
                if(tempobj1 == null) return false; // basic NULL Handling
                Object tempobj2 = evaluateRexCall(record,(RexCall)rightOperand);
                if(tempobj2 == null) return false; // basic NULL Handling
                switch (kind) 
                {
                    case GREATER_THAN:
                        return isGreaterThanNew(tempobj1, tempobj2);
                    case LESS_THAN:
                        return isLessThanNew(tempobj1, tempobj2);
                    case EQUALS:
                        return isEqualToNew(tempobj1, tempobj2);
                    case NOT_EQUALS:
                        return !isEqualToNew(tempobj1, tempobj2);
                    case GREATER_THAN_OR_EQUAL:
                        return isGreaterThanNew(tempobj1, tempobj2) || isEqualToNew(tempobj1, tempobj2);
                    case LESS_THAN_OR_EQUAL:
                        return isLessThanNew(tempobj1, tempobj2) || isEqualToNew(tempobj1, tempobj2);
                    default:
                        throw new IllegalStateException("Predicate not supported yet");
                }
            }
            else 
            {
                throw new IllegalStateException("Predicate not supported yet");
            }

        }

        private boolean isGreaterThanNew(Object o1, Object o2) 
        {
            return ((Comparable)o1).compareTo((Comparable)o2) > 0;
        }
        private boolean isLessThanNew(Object o1, Object o2) 
        {
            return ((Comparable)o1).compareTo((Comparable)o2) < 0;
        }
        private boolean isEqualToNew(Object o1, Object o2) 
        {
            return ((Comparable)o1).compareTo((Comparable)o2) == 0;
        }

        private boolean isGreaterThan(Object o, RexLiteral rexLiteral) 
        {
            return ((Comparable)o).compareTo(rexLiteral.getValueAs(o.getClass())) > 0;
        }

        private boolean isLessThan(Object o, RexLiteral rexLiteral) 
        {
            return ((Comparable)o).compareTo(rexLiteral.getValueAs(o.getClass())) < 0;
        }

        private boolean isEqualTo(Object o, RexLiteral rexLiteral) 
        {
            try 
            {
                return ((Comparable)o).compareTo(rexLiteral.getValueAs(o.getClass())) == 0;
            } 
            catch (Exception e) 
            {
                throw new IllegalStateException("Predicate not supported yet");
            }
        }

    }

}
