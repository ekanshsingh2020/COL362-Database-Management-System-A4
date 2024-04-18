package rel;

import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;

import convention.PConvention;

public class PSort extends Sort implements PRel{
    
    public PSort(
            RelOptCluster cluster,
            RelTraitSet traits,
            List<RelHint> hints,
            RelNode child,
            RelCollation collation,
            RexNode offset,
            RexNode fetch
            ) {
        super(cluster, traits, hints, child, collation, offset, fetch);
        assert getConvention() instanceof PConvention;
    }

    @Override
    public Sort copy(RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
        return new PSort(getCluster(), traitSet, hints, input, collation, offset, fetch);
    }

    @Override
    public String toString() {
        return "PSort";
    }
    int index;
    int limit;
    List<Object[]> lis;

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open(){
        logger.trace("Opening PSort");
        /* Write your code here */
        System.out.println("PSort opened");
        // System.out.println(this.collation);
        index = 0;
        lis = new ArrayList<Object[]>();
        limit = this.fetch == null ? 1000000000 : Integer.parseInt(this.fetch.toString());
        // System.out.println("Limit: " + limit);

        // System.out.println(this.explain());
        // System.out.println(this.hints);
        // System.out.println(this.rowType);


        if (this.input instanceof PRel) {
            PRel inputNode = (PRel) this.input;
            return inputNode.open();
        }
        return false;
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PSort");
        System.out.println("Closing PSort");
        if (this.input instanceof PRel) {
            PRel inputNode = (PRel) this.input;
            inputNode.close();
        }
        return;
    }


    // returns true if there is a next row, false otherwise


    public void sortList(List<Object[]> lis, int columnIndex, boolean descending) {
        // sort the list based on the column index
        // if descending is true, sort in descending order
        // else sort in ascending order
        Comparator<Object[]> comparator = new Comparator<Object[]>() {
        @Override
        public int compare(Object[] row1, Object[] row2) {
            // Get the values at the specified column index for comparison
            Comparable<Object> value1 = (Comparable<Object>) row1[columnIndex];
            Comparable<Object> value2 = (Comparable<Object>) row2[columnIndex];

            int result = value1.compareTo(value2);
            return descending ? -result : result;
        }
    };

    // Sort the list using the defined Comparator
        Collections.sort(lis, comparator);
    }


    @Override
    public boolean hasNext(){
        logger.trace("Checking if PSort has next");

        if (this.input instanceof PRel) {
            PRel inputNode = (PRel) this.input;
            // System.out.println("in PSort hasNext");

            if(index == 0)
            {
                while(inputNode.hasNext())
                {
                    lis.add(inputNode.next());
                }
                System.out.println("Sorting the list");
                RelCollation collation = this.collation;
                int relColSize = collation.getFieldCollations().size();
                // for(int i = (relColSize-1); i >=0; i--)
                // {
                //     int columnIndex = collation.getFieldCollations().get(i).getFieldIndex();
                //     boolean descending = collation.getFieldCollations().get(i).getDirection().isDescending();
                    
                //     sortList(lis, columnIndex, descending);
                // }

                List<Comparator<Object[]>> comparators = new ArrayList<Comparator<Object[]>>();
                for(int i = 0; i < relColSize; i++)
                {
                    int columnIndex = collation.getFieldCollations().get(i).getFieldIndex();
                    boolean descending = collation.getFieldCollations().get(i).getDirection().isDescending();
                    Comparator<Object[]> comparator = new Comparator<Object[]>() {
                        @Override
                        public int compare(Object[] row1, Object[] row2) {
                            // Get the values at the specified column index for comparison
                            Comparable<Object> value1 = (Comparable<Object>) row1[columnIndex];
                            Comparable<Object> value2 = (Comparable<Object>) row2[columnIndex];

                            int result = value1.compareTo(value2);
                            return descending ? -result : result;
                        }
                    };
                    comparators.add(comparator);
                }
                
                Comparator<Object[]> finalComparator = new Comparator<Object[]>() {
                    @Override
                    public int compare(Object[] row1, Object[] row2) {
                        for(Comparator<Object[]> comparator : comparators)
                        {
                            int result = comparator.compare(row1, row2);
                            if(result != 0)
                                return result;
                        }
                        return 0;
                    }
                };
                
                Collections.sort(lis, finalComparator);
                
            }




            if(index < lis.size() && index < limit)
                return true;
            System.out.println("No more rows in PSort");
            return false;
        }

        return false;
    }

    // returns the next row
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PSort");
        
        if(index < lis.size())
        {
            Object [] temp = lis.get(index);
            index++;
            return temp;
        }
        return null;
    }

}
