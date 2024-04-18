package executor;

import org.apache.calcite.rel.RelNode;

import rel.PRel;

import java.util.ArrayList;
import java.util.List;

// MyCalciteConnection will create an object of this class and use it to execute the query
public class QueryExecutor {
    
    public List<Object []> execute(RelNode relNode) {

        PRel pRel = (PRel) relNode;
        boolean isOpen = pRel.open();
        if(!isOpen) {
            return null;
        }
        List<Object[]> result = new ArrayList<>();
        while(pRel.hasNext()) {
            // System.out.println("In QueryExecutor");
            Object[] row = pRel.next();
            // for(Object obj : row) {
            //     System.out.print(obj + " ");
            // }
            // System.out.println();
            result.add(row);
        }
        pRel.close();
        return result;

    }
}
