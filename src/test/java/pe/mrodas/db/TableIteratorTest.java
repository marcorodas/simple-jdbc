package pe.mrodas.db;

import org.junit.Test;

import pe.mrodas.db.helper.TableIterator;

public class TableIteratorTest {

    @Test
    public void test() {
        TableIterator tableIterator = new TableIterator(5, 3);
        outerloop:
        for (Integer row : tableIterator.getRowIterator()) {
            for (Integer col : tableIterator.getColIterator()) {
                System.out.println(String.format("Row: %s - Col: %s", row, col));
                if (row == 2 && col == 1) break outerloop;
            }
        }
        System.out.println(String.format("Stopped in: row=%s col=%s", tableIterator.getPosRow(), tableIterator.getPosCol()));
    }
}