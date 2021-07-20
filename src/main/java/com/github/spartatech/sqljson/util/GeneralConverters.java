package com.github.spartatech.sqljson.util;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Optional;

public class GeneralConverters {

    public static Double convertToDouble(Object obj) {
        if (obj instanceof Long) {
            return ((Long) obj).doubleValue();
        } else if (obj instanceof Double) {
             return (Double)obj;
        } else {
            throw new NumberFormatException(obj + "("+obj.getClass() + ") is not convertible to Double");
        }
    }

    public static Optional<LocalDateTime> convertTextToLocalDateTime(String strValue) {
        try {
            return Optional.of(LocalDateTime.parse(strValue, DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        } catch(DateTimeParseException e) {
            return Optional.empty();
        }
    }

    public static Optional<Instant> convertTextToInstant(String strValue) {
        try {
            return Optional.of(Instant.parse(strValue));
        } catch(DateTimeParseException e) {
            return Optional.empty();
        }
    }

    public static Optional<LocalDate> convertTextToLocalDate(String strValue) {
        try {
            return Optional.of(LocalDate.parse(strValue, DateTimeFormatter.ISO_LOCAL_DATE));
        } catch(DateTimeParseException e) {
            return Optional.empty();
        }
    }

    public static Optional<LocalTime> convertTextToLocalTime(String strValue) {
        try {
            return Optional.of(LocalTime.parse(strValue, DateTimeFormatter.ISO_LOCAL_TIME));
        } catch(DateTimeParseException e) {
            return Optional.empty();
        }
    }
}
