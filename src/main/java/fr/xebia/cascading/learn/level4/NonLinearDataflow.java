package fr.xebia.cascading.learn.level4;

import cascading.flow.FlowDef;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.pipe.joiner.InnerJoin;
import cascading.tap.Tap;
import cascading.tap.hadoop.TemplateTap;
import cascading.tuple.Fields;

/**
 * Up to now, operations were stacked one after the other. But the dataflow can
 * be non linear, with multiples sources, multiples sinks, forks and merges.
 */
public class NonLinearDataflow {
	
	/**
	 * Use {@link CoGroup} in order to know the party of each presidents.
	 * You will need to create (and bind) one Pipe per source.
	 * You might need to correct the schema in order to match the expected results.
	 * 
	 * presidentsSource field(s) : "year","president"
	 * partiesSource field(s) : "year","party"
	 * sink field(s) : "president","party"
	 * 
	 * @see http://docs.cascading.org/cascading/2.1/userguide/html/ch03s03.html#N20650
	 */
	public static FlowDef cogroup(Tap<?, ?, ?> presidentsSource, Tap<?, ?, ?> partiesSource,
			Tap<?, ?, ?> sink) {

        Pipe pres = new Pipe("pres");
        Pipe party = new Pipe("party");

        Fields joinField = new Fields("year");
        Fields declared = new Fields("year_pres","president","year_party","party");
        Pipe coGroup = new CoGroup(pres, joinField, party, joinField,declared, new InnerJoin());
        Pipe discard = new Discard(coGroup,new Fields("year_pres","year_party"));
        Pipe sinkPipe = new Pipe("sink",discard);
        return FlowDef.flowDef()//
                .addSource(pres, presidentsSource)
                .addSource(party, partiesSource)//
                .addTailSink(sinkPipe,sink);
    }

    /**
	 * Split the input in order use a different sink for each party. There is no
	 * specific operator for that, use the same Pipe instance as the parent.
	 * You will need to create (and bind) one named Pipe per sink.
	 * 
	 * source field(s) : "president","party"
	 * gaullistSink field(s) : "president","party"
	 * republicanSink field(s) : "president","party"
	 * socialistSink field(s) : "president","party"
	 * 
	 * In a different context, one could use {@link TemplateTap} in order to arrive to a similar results.
	 * @see http://docs.cascading.org/cascading/2.1/userguide/htmlsingle/#N214FF
	 */

    // not able to imlp as of now TODO
    public static FlowDef split(Tap<?, ?, ?> source,
			Tap<?, ?, ?> gaullistSink, Tap<?, ?, ?> republicanSink, Tap<?, ?, ?> socialistSink) {

        Pipe parent = new Pipe("parent");

        Pipe gaul = new Each(parent, new ExpressionFilter("party.equals(\"gaullist\")", String.class));
        Pipe rep = new Each(parent, new ExpressionFilter("party.equals(\"republican\")", String.class));
        Pipe soc = new Each(parent, new ExpressionFilter("party.equals(\"socialist\")", String.class));

        return FlowDef.flowDef()//
                .addSource(parent, source)
                .addSink(gaul, gaullistSink)
                .addSink(rep, republicanSink)
                .addSink(soc, socialistSink);


    }

}
