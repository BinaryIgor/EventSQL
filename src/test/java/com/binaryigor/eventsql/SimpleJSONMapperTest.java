package com.binaryigor.eventsql;

import com.binaryigor.eventsql.internal.sql.SimpleJSONMapper;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class SimpleJSONMapperTest {

    @Test
    void mapsToJSON() {
        var input = new LinkedHashMap<String, String>();
        input.put("id", "1");
        input.put("name", "some-name");
        input.put("description", """
                line 1
                line 2""");

        var expectedJson = """
                {
                  "id": "1",
                  "name": "some-name",
                  "description": "line 1\\nline 2"
                }
                """.trim();

        assertThat(SimpleJSONMapper.toJSON(input))
                .isEqualTo(expectedJson);
    }
}
