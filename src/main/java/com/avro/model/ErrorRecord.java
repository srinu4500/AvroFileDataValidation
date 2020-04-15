package com.avro.model;

public class ErrorRecord {
	private long totalNumberOfIssues;
	private int id;
	private String regExFailedField;
	private String firstNameFailed;
	private String lastNameFailed;

	public long getTotalNumberOfIssues() {
		return totalNumberOfIssues;
	}

	public void setTotalNumberOfIssues(long totalNumberOfIssues) {
		this.totalNumberOfIssues = totalNumberOfIssues;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getRegExFailedField() {
		return regExFailedField;
	}

	public void setRegExFailedField(String regExFailedField) {
		this.regExFailedField = regExFailedField;
	}

	public String getFirstNameFailed() {
		return firstNameFailed;
	}

	public void setFirstNameFailed(String firstNameFailed) {
		this.firstNameFailed = firstNameFailed;
	}

	public String getLastNameFailed() {
		return lastNameFailed;
	}

	public void setLastNameFailed(String lastNameFailed) {
		this.lastNameFailed = lastNameFailed;
	}

}
