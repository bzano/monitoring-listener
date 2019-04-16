package org.apache.oozie.event.listener.monitoring.json;

import java.io.IOException;
import java.util.Date;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

public class DateTypeAdapter extends TypeAdapter<Date> {
	@Override
	public Date read(JsonReader in) throws IOException {
		return null;
	}

	@Override
	public void write(JsonWriter out, Date value) throws IOException {
		if (value != null) {
			out.value(value.getTime());
		} else {
			out.nullValue();
		}
	}
}
