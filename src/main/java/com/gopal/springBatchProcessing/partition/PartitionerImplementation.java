package com.gopal.springBatchProcessing.partition;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

@Component
public class PartitionerImplementation implements Partitioner {
    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        File file = new File(System.getProperty("user.dir") + "\\src\\main\\resources\\data.csv");

        Map<String, ExecutionContext> result = new HashMap<>();
        int min = 1;
        int max = Integer.parseInt(String.valueOf(file.length()));
        int targestSize = (max - min) / gridSize + 1;
        int number = 0;
        int start = min;
        int end = start + targestSize - 1;
        while (start <= max) {
            ExecutionContext value = new ExecutionContext();
            result.put("partition" + number++, value);
            if (end >= max)
                end = max;
            value.putInt("minValue", start);
            value.putInt("maxValue", end);
            start += targestSize;
            end += targestSize;
        }
        System.out.println("Partition result : " + result);
        return result;
    }
}
