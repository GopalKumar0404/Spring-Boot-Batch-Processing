package com.gopal.springBatchProcessing;

import com.gopal.springBatchProcessing.entity.Customer;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class demoMain {
    public static List<Customer>  customers = new ArrayList<>();
    private static String userDirectory=System.getProperty("user.dir")+"\\src\\main\\resources";

    public static void main(String[] args) {
//        System.out.println((5&6)^(5|6));
//        int num = true ? ~-6:5<<1;
//        int out = 5;
//        System.out.println(num);
        readCustomersFromCSV();
        writeCustomersToCSV();
    }
    public static void readCustomersFromCSV() {
        String line;
        try (BufferedReader br = new BufferedReader(new FileReader(userDirectory+"\\data.csv"))) {
            String headerLine = br.readLine();
            String[] headers = headerLine.split(",");
            while ((line = br.readLine()) != null) {
                Customer customer = getCustomer(line, headers);
                customer.setId(UUID.randomUUID().toString());
                customers.add(customer);
            }
        } catch (IOException | NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
            System.err.println("Error reading file: " + e.getMessage());
        }
    }

    private static Customer getCustomer(String line, String[] headers) throws NoSuchFieldException, IllegalAccessException {
        String[] values = line.split(",");
        Customer customer = new Customer();
        for (int i = 0; i < headers.length; i++) {
            Field field = Customer.class.getDeclaredField(headers[i]);
            field.setAccessible(true);
            String value = values[i];
            if (field.getType() == int.class) {
                field.setInt(customer, Integer.parseInt(value));
            } else {
                field.set(customer, value);
            }
        }
        return customer;
    }

    public static void writeCustomersToCSV() {
        try (PrintWriter writer = new PrintWriter(new FileWriter(userDirectory+"\\fileName.csv"))) {
            Field[] fields = Customer.class.getDeclaredFields();
            // Write the header
            for (int i = 0; i < fields.length; i++) {
                fields[i].setAccessible(true);
                writer.print(fields[i].getName());
                if (i < fields.length - 1) {
                    writer.print(",");
                }
            }
            writer.println();
            // Write data rows
            for (Customer customer : customers) {
                for (int i = 0; i < fields.length; i++) {
                    fields[i].setAccessible(true);
                    writer.print(fields[i].get(customer));
                    if (i < fields.length - 1) {
                        writer.print(",");
                    }
                }
                writer.println();
            }
        } catch (IOException | IllegalAccessException e) {
            System.err.println("Error writing to file: " + e.getMessage());
        }
    }
}
