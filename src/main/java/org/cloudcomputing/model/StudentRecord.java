package org.cloudcomputing.model;

import org.apache.hadoop.io.Text;

public class StudentRecord {
    public String gender;
    public String schoolType;
    public String locale;
    public double gpa;
    public boolean internetAccess;
    public boolean partTimeJob;
    public boolean romantic;
    public int freeTime;

    public StudentRecord(Text value) throws IllegalArgumentException {
        try {
            String[] fields = value.toString().split(",");

            // Check for header
            if (fields[0].trim().equalsIgnoreCase("Age")) {
                throw new IllegalArgumentException("Header row");
            }

            // Expecting exactly 21 fields
            if (fields.length != 21) {
                throw new IllegalArgumentException("Expected 21 fields, but got: " + fields.length);
            }

            this.gender = fields[2];
            this.schoolType = fields[6];
            this.locale = fields[7];
            this.gpa = Double.parseDouble(fields[11]);
            this.internetAccess = fields[14].equals("1");
            this.partTimeJob = fields[16].equals("1");
            this.romantic = fields[18].equals("1");
            this.freeTime = Integer.parseInt(fields[19]);

        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse record: " + value.toString(), e);
        }
    }
}
