package com.avro.service;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Logger;

import com.avro.Customer;
import com.avro.helper.GetConfigValues;
import com.avro.model.CustomerDetails;
import com.avro.model.CustomerModel;
import com.avro.model.ErrorRecord;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.itextpdf.text.BaseColor;
import com.itextpdf.text.Document;
import com.itextpdf.text.Phrase;
import com.itextpdf.text.pdf.PdfPCell;
import com.itextpdf.text.pdf.PdfPTable;
import com.itextpdf.text.pdf.PdfWriter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class AvroFileOperations {
	private static final Logger logger = Logger.getLogger(AvroFileOperations.class);
	private static ArrayList<ErrorRecord> errorCustomers = new ArrayList<>();
	private static String outputReportFileLocation = null;
	private static String inputConfigFile = null;
	private static String inputJsonFileLocation = null;
	private static String inputAvroFileLocation = "customer-output.avro";
	// java -jar avro-jar-with-dependencies.jar customers.json config.properties .

	public static void main(String[] args) {
		logger.info("started at " + LocalTime.now());
		long start = System.currentTimeMillis();
		try {
			if (ArrayUtils.isNotEmpty(args)) {
				int length = args.length;
				logger.info("Input Params : " + length);
				inputJsonFileLocation = args[0];
				inputConfigFile = args[1];
				outputReportFileLocation = args[2];

				for (String input : args) {
					logger.info(input);
				}
			} else {
				inputJsonFileLocation = "src/main/resources/customers.json";
				inputConfigFile = "src/main/resources/config.properties";
				outputReportFileLocation = "src/main/resources/";
			}
		} catch (ArrayIndexOutOfBoundsException e) {
			logger.error(
					"Need to pass 3 input params like JSON, AVRO file Location & Generate PDF Report Location....");
			System.exit(0);
		} catch (Exception e) {
			logger.error(e.getMessage());
		} finally {
			logger.info("3 arguments passed");
		}

		ArrayList<Customer> customerList = new ArrayList<>();
		Customer customer = null;
		try {
			ObjectMapper mapper = new ObjectMapper();
			// Read the json file and put into object
			CustomerModel obj = mapper.readValue(new File(inputJsonFileLocation), CustomerModel.class);
			logger.info("Total Customers : " + obj.getCustomers().size());

			for (CustomerDetails cd : obj.getCustomers()) {
				// Build a customer
				Customer.Builder customerBuilder = Customer.newBuilder();
				customerBuilder.setId(cd.getId());
				customerBuilder.setAge(cd.getAge());
				customerBuilder.setFirstName(cd.getFirstName());
				customerBuilder.setLastName(cd.getLastName());
				customerBuilder.setAutomatedEmail(cd.getAutomatedEmail());
				customerBuilder.setHeight(cd.getHeight());
				customerBuilder.setWeight(cd.getWeight());

				customer = customerBuilder.build();
				customerList.add(customer);
			}
		} catch (JsonParseException e) {
			logger.error(e.getMessage());
		} catch (JsonMappingException e1) {
			logger.error(e1.getMessage());
		} catch (IOException e2) {
			logger.error(e2.getMessage());
		} finally {
			logger.info("successfully read the json file....");
		}

		// Write customers into avro file
		final DatumWriter<Customer> datumWriter = new SpecificDatumWriter<>(Customer.class);

		try (DataFileWriter<Customer> dataFileWriter = new DataFileWriter<>(datumWriter)) {
			if (null != customer)
				dataFileWriter.create(customer.getSchema(), new File(inputAvroFileLocation));
			for (Customer cust : customerList)
				dataFileWriter.append(cust);
		} catch (IOException e) {
			logger.error(e.getMessage());
		} finally {
			customerList = null;
			logger.info("successfully wrote customer-output.avro");
		}

		// We can read our generated avro file
		ArrayList<Customer> validCustomers = new ArrayList<>();
		ArrayList<Customer> inValidCustomers = new ArrayList<>();
		final File fileGeneric = new File(inputAvroFileLocation);
		final DatumReader<Customer> datumReaderGeneric = new SpecificDatumReader<>(Customer.class);

		try {
			DataFileReader<Customer> dataFileReaderGeneric = new DataFileReader<>(fileGeneric, datumReaderGeneric);
			while (dataFileReaderGeneric.hasNext()) {
				Customer readCustomer = dataFileReaderGeneric.next();
				if (validateCustomer(readCustomer))
					validCustomers.add(readCustomer);
				else
					inValidCustomers.add(readCustomer);
			}
			dataFileReaderGeneric.close();
		} catch (IOException e) {
			logger.error(e.getMessage());
		} finally {	
			logger.info("successfully read customer-output.avro");
		}

		if (!validCustomers.isEmpty())
			createPdf(validCustomers, "ValidCustomers");
		if (!inValidCustomers.isEmpty())
			createPdf(inValidCustomers, "InvalidCustomers");

		logger.info("Report Generated successfully.....");
		logger.info("Total Time Taken : " + (System.currentTimeMillis() - start) / 1000 + " secs");
	}

	private static boolean validateCustomer(Customer customer) {
		GetConfigValues configValues = new GetConfigValues();
		String regExp = configValues.getValueByKey("REG_EXP_NAMES", inputConfigFile);

		Predicate<String> namesCheck = cust -> Pattern.matches(regExp, cust);
		if (namesCheck.test(customer.getFirstName()) && namesCheck.test(customer.getLastName()))
			return true;
		else if (!namesCheck.test(customer.getFirstName())) {
			ErrorRecord rec = new ErrorRecord();
			rec.setId(customer.getId());
			rec.setFirstNameFailed("FirstName field not valid data");
			errorCustomers.add(rec);
			return false;
		} else if (!namesCheck.test(customer.getLastName())) {
			ErrorRecord rec = new ErrorRecord();
			rec.setId(customer.getId());
			rec.setLastNameFailed("LastName field not valid data");
			errorCustomers.add(rec);
			return false;
		} else {
			ErrorRecord rec = new ErrorRecord();
			rec.setId(customer.getId());
			rec.setRegExFailedField("Error in total record");
			errorCustomers.add(rec);
			return false;
		}
	}

	public static void createPdf(ArrayList<Customer> customers, String fileName) {
		try {
			Document document = new Document();
			PdfWriter.getInstance(document, new FileOutputStream(outputReportFileLocation + "/" + fileName + ".pdf"));

			document.open();
			PdfPTable table = null;

			if (fileName.equals("ValidCustomers")) {
				String[] tableHeaders = { "Id", "First Name", "Last Name", "Age", "AutomatedEmail", "Height",
						"Weight" };
				table = new PdfPTable(tableHeaders.length);
				addTableHeader(table, tableHeaders);
				addRows(table, customers);
			} else {
				String[] tableHeaders = { "Id", "Error Reason" };
				table = new PdfPTable(tableHeaders.length);
				addErrorTableHeader(table, tableHeaders);
				addErrorRows(table, errorCustomers);
			}
			document.add(table);
			document.close();
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	private static void addTableHeader(PdfPTable table, String[] tableHeaders) {
		Stream.of(tableHeaders).forEach(columnTitle -> {
			PdfPCell header = new PdfPCell();
			header.setBackgroundColor(BaseColor.LIGHT_GRAY);
			header.setPhrase(new Phrase(columnTitle));
			table.addCell(header);
		});
	}

	private static void addRows(PdfPTable table, ArrayList<Customer> customerList) {
		for (Customer customer : customerList) {
			table.addCell(customer.getId() + "");
			table.addCell(customer.getFirstName());
			table.addCell(customer.getLastName());
			table.addCell(customer.getAge() + "");
			table.addCell(customer.getAutomatedEmail() + "");
			table.addCell(customer.getHeight() + "");
			table.addCell(customer.getWeight() + "");
		}
	}

	// executable jar inputs 3 params avro file, config file and output pdf files
	// zip file
	private static void addErrorTableHeader(PdfPTable table, String[] tableHeaders) {
		Stream.of(tableHeaders).forEach(columnTitle -> {
			PdfPCell header = new PdfPCell();
			header.setBackgroundColor(BaseColor.LIGHT_GRAY);
			header.setPhrase(new Phrase(columnTitle));
			table.addCell(header);
		});
	}

	private static void addErrorRows(PdfPTable table, ArrayList<ErrorRecord> customerList) {
		for (ErrorRecord customer : customerList) {
			table.addCell(customer.getId() + "");
			if (customer.getRegExFailedField() != null && !customer.getRegExFailedField().isEmpty())
				table.addCell(customer.getRegExFailedField());
			else if (customer.getFirstNameFailed() != null && !customer.getFirstNameFailed().isEmpty()) {
				table.addCell(customer.getFirstNameFailed());
			} else if (customer.getLastNameFailed() != null && !customer.getLastNameFailed().isEmpty()) {
				table.addCell(customer.getLastNameFailed());
			} else {
				table.addCell("Error");
			}
		}
	}
}