package pe.mrodas.jdbc;

import org.junit.Test;

/**
 *
 * @author skynet
 */
public class SqlQueryTest extends TestBase {

    @Override
    protected String getDefaultConfigFile() {
        return null;
    }

    private SqlQuery<Consensus> getConsensusQuery() {
        return new SqlQuery<Consensus>().setSql(new String[]{
            "SELECT",
            "   repeat_consensus_id,  repeat_class,  repeat_type, repeat_consensus",
            "   FROM repeat_consensus",
            "	WHERE repeat_consensus <> :repeat_consensus",
            "	LIMIT 5"
        }).addParameter("repeat_consensus", "N");
    }

    private void print(Consensus consensus) {
        System.out.format("%-15s%-15s%-15s%-15s\n", new Object[]{
            consensus.getRepeat_consensus_id().toString(),
            consensus.getRepeat_class(),
            consensus.getRepeat_type(),
            consensus.getRepeat_consensus()
        });
    }

    @Test
    public void testOneObject() throws Exception {
        Consensus consensus = this.getConsensusQuery().execute(Consensus.class, (rs, result) -> {
            if (rs.next()) {
                result.setRepeat_consensus_id(rs.getInt("repeat_consensus_id"));
                result.setRepeat_class(rs.getString("repeat_class"));
                result.setRepeat_type(rs.getString("repeat_type"));
                result.setRepeat_consensus(rs.getString("repeat_consensus"));
            }
        });
        this.print(consensus);
    }

    @Test
    public void testListObject() throws Exception {
        this.getConsensusQuery().execute((rs, list) -> {
            while (rs.next()) {
                Consensus consensus = new Consensus()
                        .setRepeat_consensus_id(rs.getInt("repeat_consensus_id"))
                        .setRepeat_class(rs.getString("repeat_class"))
                        .setRepeat_type(rs.getString("repeat_type"))
                        .setRepeat_consensus(rs.getString("repeat_consensus"));
                list.add(consensus);
            }
        }).forEach(this::print);
    }

}
