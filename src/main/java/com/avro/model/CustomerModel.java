package com.avro.model;

import java.util.ArrayList;
import java.util.List;

public class CustomerModel {

	private List<CustomerDetails> customers = new ArrayList<>();

	public List<CustomerDetails> getCustomers() {
		return customers;
	}

	public void setCustomers(List<CustomerDetails> customers) {
		this.customers = customers;
	}

	@Override
	public String toString() {
		return "CustomerModel [customers=" + customers + "]";
	}

}