package ufsc.bigdata;


import java.io.Serializable;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class MyTweet implements Serializable{

	private static final long serialVersionUID = 1L;

	@QuerySqlField(index = true)
	private String id;

	@QuerySqlField(index = true)
	private String username;

	@QuerySqlField(index = true)
	private String text;

	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getText() {
		return text;
	}
	public void setText(String text) {
		this.text = text;
	}

}