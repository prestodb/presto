package com.facebook.presto.operator.window;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

import java.util.List;

import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.ValueWindowFunction;
import com.facebook.presto.spi.function.WindowFunctionSignature;

import io.airlift.slice.Slice;

@WindowFunctionSignature(name = "max_step", returnType = "bigint", argumentTypes = {"varchar", "varchar"})
public class MaxStep extends ValueWindowFunction{

	private final static String delimiter = "=>";
	
	private int actionChannel;
	private int patternDefChannel;
	
	private int patternItemsSize = 0;
	private int index = 0;
	private long MAX_STEP = 0;
	private String currentSearchingItem = null;
	private String[] patternItems = null;

	public MaxStep(List<Integer> argumentChannels){
		this.actionChannel = argumentChannels.get(0);
		this.patternDefChannel = argumentChannels.get(1);
	}

    public void reset()
    {
    	this.currentSearchingItem = null;
    	this.index = 0;
    	this.MAX_STEP = 0;
    }
	
	@Override
	public void processRow(BlockBuilder output, int frameStart, int frameEnd, int currentPosition) {
        if ((frameStart < 0) || windowIndex.isNull(patternDefChannel, currentPosition)) {
        	VARCHAR.writeObject(output, null);
        }
		
		if (this.patternItems == null){
			Slice patternDefSlice = windowIndex.getSlice(patternDefChannel, currentPosition);
			String patternDef = new String(patternDefSlice.getBytes());
			this.patternItems = patternDef.split(delimiter);
			this.patternItemsSize = patternItems.length;
		}
		
		Slice valueSlice = windowIndex.getSlice(actionChannel, currentPosition);
		if(this.patternItemsSize < 1){
			INTEGER.writeObject(output, null);
		} else {
			String action = new String(valueSlice.getBytes());
			if(this.index < this.patternItemsSize){
				this.currentSearchingItem = this.patternItems[this.index];
				if(this.currentSearchingItem.equals(action)){
					this.MAX_STEP += 1;
					this.index += 1;
				}
			}
			INTEGER.writeLong(output, MAX_STEP);
		}		
	}

}
