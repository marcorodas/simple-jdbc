package pe.mrodas.jdbc;

/**
 *
 * @author skynet
 */
public class Consensus {

    private Integer repeat_consensus_id;
    private String repeat_class;
    private String repeat_type;
    private String repeat_consensus;

    public Integer getRepeat_consensus_id() {
        return repeat_consensus_id;
    }

    public Consensus setRepeat_consensus_id(Integer repeat_consensus_id) {
        this.repeat_consensus_id = repeat_consensus_id;
        return this;
    }

    public String getRepeat_class() {
        return repeat_class;
    }

    public Consensus setRepeat_class(String repeat_class) {
        this.repeat_class = repeat_class;
        return this;
    }

    public String getRepeat_type() {
        return repeat_type;
    }

    public Consensus setRepeat_type(String repeat_type) {
        this.repeat_type = repeat_type;
        return this;
    }

    public String getRepeat_consensus() {
        return repeat_consensus;
    }

    public Consensus setRepeat_consensus(String repeat_consensus) {
        this.repeat_consensus = repeat_consensus;
        return this;
    }
}
