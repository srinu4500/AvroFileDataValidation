package com.avro.model;

public class CustomerDetails {

	private int id;
	private int age;
	private String firstName;
	private String lastName;
	private Boolean automatedEmail;
	private float height;
	private float weight;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	public Boolean getAutomatedEmail() {
		return automatedEmail;
	}

	public void setAutomatedEmail(Boolean automatedEmail) {
		this.automatedEmail = automatedEmail;
	}

	public float getHeight() {
		return height;
	}

	public void setHeight(float height) {
		this.height = height;
	}

	public float getWeight() {
		return weight;
	}

	public void setWeight(float weight) {
		this.weight = weight;
	}

	@Override
	public String toString() {
		return "CustomerModel [age=" + age + ", firstName=" + firstName + ", lastName=" + lastName + ", automatedEmail="
				+ automatedEmail + ", height=" + height + ", weight=" + weight + "]";
	}
}
