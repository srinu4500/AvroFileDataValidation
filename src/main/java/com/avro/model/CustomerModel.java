package com.avro.model;

import java.util.ArrayList;

public class CustomerModel {

	private ArrayList<CustomerDetails> customers = new ArrayList<>();

	public ArrayList<CustomerDetails> getCustomers() {
		return customers;
	}

	public void setCustomers(ArrayList<CustomerDetails> customers) {
		this.customers = customers;
	}

	@Override
	public String toString() {
		return "CustomerModel [customers=" + customers + "]";
	}

}
