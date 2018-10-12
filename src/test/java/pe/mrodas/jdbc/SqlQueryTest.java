package pe.mrodas.jdbc;

import org.junit.Test;

/**
 * @author skynet
 */
public class SqlQueryTest extends TestBase {

    @Override
    protected String getDefaultConfigFile() {
        return null;
    }

    private SqlQuery<Consensus> getConsensusQuery() {
        return new SqlQuery<>(Consensus.class).setSql(new String[]{
                "SELECT",
                "   repeat_consensus_id,  repeat_class,  repeat_type, repeat_consensus",
                "   FROM repeat_consensus",
                "	WHERE repeat_consensus <> :repeat_consensus",
                "	LIMIT 5"
        }).setMapper((mapper, result, rs) -> {
            mapper.map(result::setRepeat_consensus_id, rs::getInt)
                    .map(result::setRepeat_class, rs::getString)
                    .map(result::setRepeat_type, rs::getString)
                    .map(result::setRepeat_consensus, rs::getString);
        }).addParameter("repeat_consensus", "N");
    }

    private void print(Consensus consensus) {
        System.out.format("%-15s%-15s%-15s%-15s\n",
                consensus.getRepeat_consensus_id().toString(),
                consensus.getRepeat_class(),
                consensus.getRepeat_type(),
                consensus.getRepeat_consensus()
        );
    }

    @Test
    public void testOneObject() throws Exception {
        this.print(this.getConsensusQuery().executeFirst());
    }

    @Test
    public void testListObject() throws Exception {
        this.getConsensusQuery()
                .executeList()
                .forEach(this::print);
    }

}
