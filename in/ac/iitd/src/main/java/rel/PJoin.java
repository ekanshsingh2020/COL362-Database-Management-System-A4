package rel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexCall;
import java.util.ArrayList;
import java.util.HashMap;
import convention.PConvention;

import java.util.List;
import java.util.Set;

/*
    * Implement Hash Join
    * The left child is blocking, the right child is streaming
*/
public class PJoin extends Join implements PRel {

    public PJoin(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            Set<CorrelationId> variablesSet,
            JoinRelType joinType) {
                super(cluster, traitSet, ImmutableList.of(), left, right, condition, variablesSet, joinType);
                assert getConvention() instanceof PConvention;
    }

    @Override
    public PJoin copy(
            RelTraitSet relTraitSet,
            RexNode condition,
            RelNode left,
            RelNode right,
            JoinRelType joinType,
            boolean semiJoinDone) {
        return new PJoin(getCluster(), relTraitSet, left, right, condition, variablesSet, joinType);
    }

    @Override
    public String toString() {
        return "PJoin";
    }
    int index;
    List<Object[]> lis;
    HashMap<Object[], List<Object[]>> map;
    HashMap<Object[], Boolean> mapLeft;
    int flag;
    List<Object[]> listForCar;
    List<Integer> leftList,rightList;
    // returns true if successfully opened, false otherwise
    @Override
    public boolean open() {
        
        logger.trace("Opening PJoin");
        System.out.println("PJoin opened");
        // System.out.println(this.joinType);
        // System.out.println(this.condition);
        // System.out.println(this.left);
        // System.out.println(this.right);

        map = new HashMap<Object[], List<Object[]>>();
        mapLeft = new HashMap<Object[], Boolean>();
        index = 0;
        lis = new ArrayList<Object[]>();
        flag = 0;
        
        leftList = new ArrayList<>();
        rightList = new ArrayList<>();
        if(this.condition.toString().equals("true"))
            System.out.println("No condition");
        else
        {

            RexCall cond = (RexCall)this.getCondition();
            // System.out.println("The cond is "+ cond.op);
    
            if(cond.op.toString().equals("AND"))
            {
                System.out.println("AND condition");
                for(RexNode node : cond.getOperands())
                {
                    RexCall tempNode = (RexCall)node;
                    int leftIndex = Integer.parseInt(tempNode.getOperands().get(0).toString().substring(1));
                    int rightIndex = Integer.parseInt(tempNode.getOperands().get(1).toString().substring(1))-((PRel)this.left).getRowType().getFieldCount();
                    leftList.add(leftIndex);
                    rightList.add(rightIndex);
                }
            }
            else
            {
                RexCall tempNode = (RexCall)cond;

                if(tempNode.op.toString().equals("="))
                {
                    int leftIndex = Integer.parseInt(tempNode.getOperands().get(0).toString().substring(1));
                    int rightIndex = Integer.parseInt(tempNode.getOperands().get(1).toString().substring(1))-((PRel)this.left).getRowType().getFieldCount();
                    leftList.add(leftIndex);
                    rightList.add(rightIndex);
                }
            }
        }

        // System.out.println("LeftList "+leftList);
        // System.out.println("RightList "+rightList);
        listForCar = new ArrayList<Object[]>();

        if (this.left instanceof PRel) {
            PRel leftNode = (PRel) this.left;
            return leftNode.open();
        }
        return false;
    }

    // any postprocessing, if needed
    @Override
    public void close() {
        logger.trace("Closing PJoin");
        System.out.println("Closing PJoin");
        if (this.left instanceof PRel) {
            PRel leftNode = (PRel) this.left;
            leftNode.close();
        }
        if (this.right instanceof PRel) {
            PRel rightNode = (PRel) this.right;
            rightNode.close();
        }
        return;
    }

    public String objArrayToString(Object[] obj)
    {
        String str = "";
        for(Object o : obj)
        {
            if(o == null)
                str += "null";
            else
                str += o.toString();
        }
        return str;
    }


    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext() {
        logger.trace("Checking if PJoin has next");
        PRel rightNode = (PRel) this.right;
        if(this.condition.toString().equals("true"))
        {
            if(index == 0)
            {
                if (this.left instanceof PRel) 
                {
                    PRel leftNode = (PRel) this.left;
                    while(leftNode.hasNext())
                    {
                        Object[] temp = leftNode.next();
                        if(temp!=null)
                            listForCar.add(temp);
                    }
                }
                rightNode.open();
            }

            if(flag==1)
            {
                if(index < lis.size())
                    return true;
                System.out.println("No more rows in PJoin");
                return false;
            }

            if (this.right instanceof PRel) 
            {
                while(rightNode.hasNext())
                {
                    Object[] tempnew = rightNode.next();
                    if(tempnew!=null)
                    {
                        for(Object[] left_row : listForCar)
                        {
                            Object[] result = new Object[left_row.length + tempnew.length];
                            int i = 0;
                            for(Object obj : left_row)
                            {
                                result[i] = obj;
                                i++;
                            }
                            for(int j=0;j<tempnew.length;j++)
                            {
                                result[i] = tempnew[j];
                                i++;
                            }
                            lis.add(result);
                        }
                    }
                    if(index < lis.size())
                        return true;
                }
                flag = 1;
                if(index < lis.size())
                    return true;
                System.out.println("No more rows in PJoin");
            }
        }

        if(index == 0)
        {
            if (this.left instanceof PRel) 
            {
                PRel leftNode = (PRel) this.left;
                while(leftNode.hasNext())
                {
                    Object[] temp = leftNode.next();
                    if(temp!=null)
                    {
                        
                        Object [] key = new Object[leftList.size()];
                        for(int i=0;i<leftList.size();i++)
                        {
                            key[i] = temp[leftList.get(i)];
                        }

                        String strKey = objArrayToString(key);

                        int tempFlag = 0;
                        
                        for(Object [] key1 : map.keySet())
                        {
                            String strKey1 = objArrayToString(key1);
                            if(strKey.equals(strKey1))
                            {
                                map.get(key1).add(temp);
                                tempFlag = 1;
                                break;
                            }
                        }

                        if(tempFlag == 0)
                        {
                            List<Object []> temp_list = new ArrayList<>();
                            temp_list.add(temp);
                            map.put(key, temp_list);
                            mapLeft.put(key, true);
                        }

                    }
                }
                System.out.println("Map size "+map.size());
            }
            rightNode.open();
        }
        if(this.joinType == JoinRelType.INNER)
        {
            if (this.right instanceof PRel) 
            {
                if(flag==1)
                {
                    if(index < lis.size())
                        return true;
                    System.out.println("No more rows in PJoin");
                    return false;
                }
                while(rightNode.hasNext())
                {
                    Object[] tempnew = rightNode.next();

                    if(tempnew!=null)
                    {
                        Object [] keyRight = new Object[leftList.size()];

                        for(int i=0;i<leftList.size();i++)
                        {
                            keyRight[i] = tempnew[rightList.get(i)];
                        }
                        String strKeyRight = objArrayToString(keyRight);
                        int tempoFlag = 0;

                        for(Object [] key1 : map.keySet())
                        {
                            String strKey1 = objArrayToString(key1);
                            if(strKeyRight.equals(strKey1))
                            {
                                List<Object[]> temp_list = map.get(key1);
                                for(Object[] left_row : temp_list)
                                {
                                    Object[] result = new Object[left_row.length + tempnew.length];
                                    int i = 0;
                                    for(Object obj : left_row)
                                    {
                                        result[i] = obj;
                                        i++;
                                    }
                                    for(int j=0;j<tempnew.length;j++)
                                    {
                                        result[i] = tempnew[j];
                                        i++;
                                    }
                                    lis.add(result);
                                }
                                tempoFlag = 1;
                                break;
                            }
                        }
                        if(index < lis.size())
                            return true;
                    }
                }
                flag = 1;
                if(index < lis.size())
                    return true;
                System.out.println("No more rows in PJoin");
            }
            return false;
        }
        else if (this.joinType == JoinRelType.LEFT)
        {
            if (this.right instanceof PRel) 
            {
                if(flag==1)
                {
                    if(index < lis.size())
                        return true;
                    System.out.println("No more rows in PJoin");
                    return false;
                }

                while(rightNode.hasNext())
                {
                    Object[] tempnew = rightNode.next();
                    if(tempnew!=null)
                    {
                        Object [] keyRight = new Object[leftList.size()];

                        for(int i=0;i<leftList.size();i++)
                        {
                            keyRight[i] = tempnew[rightList.get(i)];
                        }

                        String strKeyRight = objArrayToString(keyRight);


                        for(Object [] key1 : map.keySet())
                        {
                            String strKey1 = objArrayToString(key1);
                            if(strKeyRight.equals(strKey1))
                            {
                                mapLeft.put(key1, false);
                                List<Object[]> temp_list = map.get(key1);
                                for(Object[] left_row : temp_list)
                                {
                                    Object[] result = new Object[left_row.length + tempnew.length];
                                    int i = 0;
                                    for(Object obj : left_row)
                                    {
                                        result[i] = obj;
                                        i++;
                                    }
                                    for(int j=0;j<tempnew.length;j++)
                                    {
                                        result[i] = tempnew[j];
                                        i++;
                                    }
                                    lis.add(result);
                                }
                                break;
                            }
                        }


                        if(index < lis.size())
                            return true;
                    }
                }
                if(flag==0)
                {
                    for(Object[] key : mapLeft.keySet())
                    {
                        if(mapLeft.get(key))
                        {
                            List<Object[]> temp_list = map.get(key);
                            for(Object[] left_row : temp_list)
                            {
                                Object[] result = new Object[left_row.length + ((PRel)this.right).getRowType().getFieldCount()];
                                int i = 0;
                                for(Object obj : left_row)
                                {
                                    result[i] = obj;
                                    i++;
                                }
                                for(int j=0;j<((PRel)this.right).getRowType().getFieldCount();j++)
                                {
                                    result[i] = null;
                                    i++;
                                }
                                lis.add(result);
                            }
                        }
                    }
                    System.out.println(lis.size());
                    flag = 1;
                }
                if(index < lis.size())
                    return true;
                System.out.println("No more rows in PJoin");
                return false;
            }
            return false;
        }
        else if (this.joinType == JoinRelType.RIGHT)
        {
            if (this.right instanceof PRel) 
            {
                if(flag==1)
                {
                    if(index < lis.size())
                        return true;
                    System.out.println("No more rows in PJoin");
                    return false;
                }
                while(rightNode.hasNext())
                {
                    Object[] tempnew = rightNode.next();
                    if(tempnew!=null)
                    {
                        Object [] keyRight = new Object[leftList.size()];

                        for(int i=0;i<leftList.size();i++)
                        {
                            keyRight[i] = tempnew[rightList.get(i)];
                        }

                        String strKeyRight = objArrayToString(keyRight);

                        int tempoFlag = 0;

                        for(Object [] key1 : map.keySet())
                        {
                            String strKey1 = objArrayToString(key1);
                            if(strKeyRight.equals(strKey1))
                            {
                                List<Object[]> temp_list = map.get(key1);
                                for(Object[] left_row : temp_list)
                                {
                                    Object[] result = new Object[left_row.length + tempnew.length];
                                    int i = 0;
                                    for(Object obj : left_row)
                                    {
                                        result[i] = obj;
                                        i++;
                                    }
                                    for(int j=0;j<tempnew.length;j++)
                                    {
                                        result[i] = tempnew[j];
                                        i++;
                                    }
                                    lis.add(result);
                                }
                                tempoFlag = 1;
                                break;
                            }
                        }


                        if(tempoFlag == 0)
                        {
                            Object[] result = new Object[((PRel)this.left).getRowType().getFieldCount() + tempnew.length];
                            int i = 0;
                            for(int j=0;j<((PRel)this.left).getRowType().getFieldCount();j++)
                            {
                                result[i] = null;
                                i++;
                            }
                            for(int j=0;j<tempnew.length;j++)
                            {
                                result[i] = tempnew[j];
                                i++;
                            }
                            lis.add(result);
                        }

                        if(index < lis.size())
                            return true;
                    }
                }
                flag = 1;
                if(index < lis.size())
                    return true;
                System.out.println("No more rows in PJoin");
            }
            return false;
        }
        else if (this.joinType == JoinRelType.FULL)
        {
            if (this.right instanceof PRel) 
            {
                if(flag==1)
                {
                    if(index < lis.size())
                        return true;
                    System.out.println("No more rows in PJoin");
                    return false;
                }

                while(rightNode.hasNext())
                {
                    Object[] tempnew = rightNode.next();
                    if(tempnew!=null)
                    {
                        Object [] keyRight = new Object[leftList.size()];

                        for(int i=0;i<leftList.size();i++)
                        {
                            keyRight[i] = tempnew[rightList.get(i)];
                        }

                        String strKeyRight = objArrayToString(keyRight);

                        int tempoFlag = 0;

                        for(Object [] key1 : map.keySet())
                        {
                            String strKey1 = objArrayToString(key1);
                            if(strKeyRight.equals(strKey1))
                            {
                                List<Object[]> temp_list = map.get(key1);
                                for(Object[] left_row : temp_list)
                                {
                                    Object[] result = new Object[left_row.length + tempnew.length];
                                    int i = 0;
                                    for(Object obj : left_row)
                                    {
                                        result[i] = obj;
                                        i++;
                                    }
                                    for(int j=0;j<tempnew.length;j++)
                                    {
                                        result[i] = tempnew[j];
                                        i++;
                                    }
                                    lis.add(result);
                                }
                                tempoFlag = 1;
                                break;
                            }
                        }


                        if(tempoFlag == 0)
                        {
                            Object[] result = new Object[((PRel)this.left).getRowType().getFieldCount() + tempnew.length];
                            int i = 0;
                            for(int j=0;j<((PRel)this.left).getRowType().getFieldCount();j++)
                            {
                                result[i] = null;
                                i++;
                            }
                            for(int j=0;j<tempnew.length;j++)
                            {
                                result[i] = tempnew[j];
                                i++;
                            }
                            lis.add(result);
                        }

                        if(index < lis.size())
                            return true;
                    }
                }
                // traverse the mapLeft and find the key value pair which has value as true
                if(flag==0)
                {
                    for(Object[] key : mapLeft.keySet())
                    {
                        if(mapLeft.get(key))
                        {
                            List<Object[]> temp_list = map.get(key);
                            for(Object[] left_row : temp_list)
                            {
                                Object[] result = new Object[left_row.length + ((PRel)this.right).getRowType().getFieldCount()];
                                int i = 0;
                                for(Object obj : left_row)
                                {
                                    result[i] = obj;
                                    i++;
                                }
                                for(int j=0;j<((PRel)this.right).getRowType().getFieldCount();j++)
                                {
                                    result[i] = null;
                                    i++;
                                }
                                lis.add(result);
                            }
                        }
                    }
                    System.out.println(lis.size());
                    flag = 1;
                }
                if(index < lis.size())
                    return true;
                System.out.println("No more rows in PJoin");
            }
            return false;
        }
        else
            return false;
    }

    // returns the next row
    @Override
    public Object[] next() {
        logger.trace("Getting next row from PJoin");
        if(index < lis.size())
        {
            Object [] tempo = lis.get(index);
            index++;
            return tempo;
        }
        return null;
    }
}



