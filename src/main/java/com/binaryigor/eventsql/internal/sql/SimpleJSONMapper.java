package com.binaryigor.eventsql.internal.sql;

import java.util.Map;
import java.util.stream.Collectors;

public class SimpleJSONMapper {

    public static String toJSON(Map<String, String> map) {
        return map.entrySet().stream()
                .map(e -> "  \"%s\": \"%s\"".formatted(e.getKey(), escaped(e.getValue())))
                .collect(Collectors.joining(",\n", "{\n", "\n}"));
    }

    // mostly copied from org.jooq.tools.json.JSONValue.escape
    private static String escaped(String s) {
        if (s == null) {
            return null;
        }
        var sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            switch (ch) {
                case '"':
                    sb.append("\\\"");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\b':
                    sb.append("\\b");
                    break;
                case '\f':
                    sb.append("\\f");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                case '/':
                    sb.append("\\/");
                    break;
                default:
                    // Reference: http://www.unicode.org/versions/Unicode5.1.0/
                    if ((ch >= '\u0000' && ch <= '\u001F') || (ch >= '\u007F' && ch <= '\u009F')
                            || (ch >= '\u2000' && ch <= '\u20FF')) {
                        String ss = Integer.toHexString(ch);
                        sb.append("\\u");
                        for (int k = 0; k < 4 - ss.length(); k++) {
                            sb.append('0');
                        }
                        sb.append(ss.toUpperCase());
                    } else {
                        sb.append(ch);
                    }
            }
        }
        return sb.toString();
    }
}
