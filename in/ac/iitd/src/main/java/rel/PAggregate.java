package rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.util.ImmutableBitSet;

import convention.PConvention;
import edu.emory.mathcs.backport.java.util.Arrays;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

// Count, Min, Max, Sum, Avg
public class PAggregate extends Aggregate implements PRel {

    public PAggregate(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelHint> hints,
            RelNode input,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls) {
        super(cluster, traitSet, hints, input, groupSet, groupSets, aggCalls);
        assert getConvention() instanceof PConvention;
    }

    @Override
    public Aggregate copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet,
                          List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        return new PAggregate(getCluster(), traitSet, hints, input, groupSet, groupSets, aggCalls);
    }

    @Override
    public String toString() {
        return "PAggregate";
    }

    List<Object []> lis;
    List<Object []> templis;
    HashMap <Object[], List<Object[]>> map;
    List<Integer> groupBySet;
    int index;


    // returns true if successfully opened, false otherwise
    @Override
    public boolean open() 
    {
        logger.trace("Opening PAggregate");
        System.out.println("PAggregate opened");
        System.out.println(this.groupSet);
        System.out.println(this.aggCalls);
        System.out.println(this.input);
        lis = new ArrayList<>();
        templis = new ArrayList<>();
        map = new HashMap<>();
        groupBySet = new ArrayList<>();
        for(int i = 0; i < this.groupSet.length(); i++)
        {
            if(this.groupSet.get(i))
                groupBySet.add(i);
        }
        System.out.println("GroupBySet "+groupBySet);
        index = 0;
        if (this.input instanceof PRel) {
            PRel inputNode = (PRel) this.input;
            return inputNode.open();
        }
        return false;
    }

    // any postprocessing, if needed
    @Override
    public void close() 
    {
        logger.trace("Closing PAggregate");
        System.out.println("Closing PAggregate");
        if (this.input instanceof PRel) {
            PRel inputNode = (PRel) this.input;
            inputNode.close();
        }
        return;
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext() 
    {
        logger.trace("Checking if PAggregate has next");
        if (this.input instanceof PRel) {
            PRel inputNode = (PRel) this.input;
            
            if(index == 0)
            {
                while(inputNode.hasNext())
                    templis.add(inputNode.next());
                for(Object [] row : templis)
                {
                    Object [] key = new Object[groupBySet.size()];
                    for(int i = 0; i < groupBySet.size(); i++)
                        key[i] = row[groupBySet.get(i)];
                    String tempStrKey = Arrays.toString(key);
                    int tempFlag = 0;
                    for(Object [] key1 : map.keySet())
                    {
                        String tempStrKey1 = Arrays.toString(key1);
                        if(tempStrKey.equals(tempStrKey1))
                        {
                            map.get(key1).add(row);
                            tempFlag = 1;
                            break;
                        }
                    }
                    if(tempFlag == 0)
                    {
                        List<Object []> temp = new ArrayList<>();
                        temp.add(row);
                        map.put(key, temp);
                    }
                }
                System.out.println(map.size());
                
                for(Object [] key : map.keySet())
                {
                    List<Object []> temp = map.get(key);
                    Object [] newRow = new Object[this.aggCalls.size() + groupBySet.size()];
                    for(int i = 0; i < groupBySet.size(); i++)
                    {
                        newRow[i] = key[i];
                    }
                    for(int i = 0; i < this.aggCalls.size(); i++)
                    {
                        AggregateCall aggCall = this.aggCalls.get(i);
                        
                        switch(aggCall.getAggregation().getName())
                        {
                            case "COUNT":
                                if(aggCall.isDistinct())
                                {
                                    System.out.println("Distinct Count====================");
                                    List<Object> tempDistinct = new ArrayList<>();
                                    for(Object [] row : temp)
                                    {
                                        if(!tempDistinct.contains(row[aggCall.getArgList().get(0)]))
                                            tempDistinct.add(row[aggCall.getArgList().get(0)]);
                                    }
                                    newRow[i + groupBySet.size()] = tempDistinct.size();
                                }
                                else
                                    newRow[i + groupBySet.size()] = temp.size();
                                break;
                            case "MIN":
                                newRow[i + groupBySet.size()] = Collections.min(temp, new Comparator<Object[]>() {
                                    @Override
                                    public int compare(Object[] o1, Object[] o2) {
                                        return ((Comparable)o1[aggCall.getArgList().get(0)]).compareTo(o2[aggCall.getArgList().get(0)]);
                                    }
                                })[aggCall.getArgList().get(0)];
                                break;
                            case "MAX":
                                newRow[i + groupBySet.size()] = Collections.max(temp, new Comparator<Object[]>() {
                                    @Override
                                    public int compare(Object[] o1, Object[] o2) {
                                        return ((Comparable)o1[aggCall.getArgList().get(0)]).compareTo(o2[aggCall.getArgList().get(0)]);
                                    }
                                })[aggCall.getArgList().get(0)];
                                break;
                            case "SUM":
                                int tempsumInt = 0;
                                double tempsumDouble = 0;
                                float tempsumFloat = 0;
                                long tempsumLong = 0;
                                boolean isInt = false;
                                boolean isDouble = false;
                                boolean isFloat = false;
                                boolean isLong = false; 
                                // newRow[i + groupBySet.size()] = tempsum;
                                for(Object [] row : temp)
                                {
                                    if(row[aggCall.getArgList().get(0)] instanceof Integer)
                                    {
                                        isInt = true;
                                        tempsumInt += (int)row[aggCall.getArgList().get(0)];
                                    }
                                    else if(row[aggCall.getArgList().get(0)] instanceof Double)
                                    {
                                        isDouble = true;
                                        tempsumDouble += (double)row[aggCall.getArgList().get(0)];
                                    }
                                    else if(row[aggCall.getArgList().get(0)] instanceof Float)
                                    {
                                        isFloat = true;
                                        tempsumFloat += (float)row[aggCall.getArgList().get(0)];
                                    }
                                    else if(row[aggCall.getArgList().get(0)] instanceof Long)
                                    {
                                        isLong = true;
                                        tempsumLong += (long)row[aggCall.getArgList().get(0)];
                                    }
                                    else
                                    {
                                        isInt = true;
                                        tempsumInt += (int)row[aggCall.getArgList().get(0)];
                                    }
                                }
                                if(isInt)
                                    newRow[i + groupBySet.size()] = tempsumInt;
                                else if(isDouble)
                                    newRow[i + groupBySet.size()] = tempsumDouble;
                                else if(isFloat)
                                    newRow[i + groupBySet.size()] = tempsumFloat;
                                else if(isLong)
                                    newRow[i + groupBySet.size()] = tempsumLong;
                                else
                                    newRow[i + groupBySet.size()] = tempsumInt;
                                break;
                            case "AVG":
                                int tempavgInt = 0;
                                double tempavgDouble = 0;
                                float tempavgFloat = 0;
                                long tempavgLong = 0;
                                boolean isIntAvg = false;
                                boolean isDoubleAvg = false;
                                boolean isFloatAvg = false;
                                boolean isLongAvg = false; 
                                // newRow[i + groupBySet.size()] = tempsum;
                                for(Object [] row : temp)
                                {
                                    if(row[aggCall.getArgList().get(0)] instanceof Integer)
                                    {
                                        isIntAvg = true;
                                        tempavgInt += (int)row[aggCall.getArgList().get(0)];
                                    }
                                    else if(row[aggCall.getArgList().get(0)] instanceof Double)
                                    {
                                        isDoubleAvg = true;
                                        tempavgDouble += (double)row[aggCall.getArgList().get(0)];
                                    }
                                    else if(row[aggCall.getArgList().get(0)] instanceof Float)
                                    {
                                        isFloatAvg = true;
                                        tempavgFloat += (float)row[aggCall.getArgList().get(0)];
                                    }
                                    else if(row[aggCall.getArgList().get(0)] instanceof Long)
                                    {
                                        isLongAvg = true;
                                        tempavgLong += (long)row[aggCall.getArgList().get(0)];
                                    }
                                    else
                                    {
                                        isIntAvg = true;
                                        tempavgInt += (int)row[aggCall.getArgList().get(0)];
                                    }
                                }
                                if(isIntAvg)
                                    newRow[i + groupBySet.size()] = tempavgInt;
                                else if(isDoubleAvg)
                                    newRow[i + groupBySet.size()] = tempavgDouble;
                                else if(isFloatAvg)
                                    newRow[i + groupBySet.size()] = tempavgFloat;
                                else if(isLongAvg)
                                    newRow[i + groupBySet.size()] = tempavgLong;
                                else
                                    newRow[i + groupBySet.size()] = tempavgInt;
                                break;
                        }
                    }
                    lis.add(newRow);
                }
            }
            if(index < lis.size())
                return true;
            System.out.println("No more rows in PAggregate");
            return false;
        }
        
        return false;
    }

    // returns the next row
    @Override
    public Object[] next() {
        logger.trace("Getting next row from PAggregate");
        if (this.input instanceof PRel) {
            return lis.get(index++);
        }
        return null;
    }

}