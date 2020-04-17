package com.avro.service;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.log4j.Logger;

import com.avro.Customer;
import com.avro.exception.InvalidNumberOfArgumentsException;
import com.avro.exception.MyCustomException;
import com.avro.helper.GetConfigValues;
import com.avro.model.CustomerDetails;
import com.avro.model.CustomerModel;
import com.avro.model.ErrorRecord;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.itextpdf.text.BaseColor;
import com.itextpdf.text.Document;
import com.itextpdf.text.DocumentException;
import com.itextpdf.text.Phrase;
import com.itextpdf.text.pdf.PdfPCell;
import com.itextpdf.text.pdf.PdfPTable;
import com.itextpdf.text.pdf.PdfWriter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class AvroFileOperations {
	
	private static final Logger logger = Logger.getLogger(AvroFileOperations.class);
	private static ArrayList<ErrorRecord> errorCustomers = new ArrayList<>();
	private static String inputAvroFileLocation = "customer-output.avro";
	private String outputReportFileLocation = null;
	private String inputConfigFile = null;
	private String inputJsonFileLocation = null;
	
	// java -jar avro-jar-with-dependencies.jar customers.json config.properties .
	
	public AvroFileOperations() {
		super();
	}
	
	public AvroFileOperations(String inputJsonFileLocation, String inputConfigFile, String outputReportFileLocation) {
		this.inputJsonFileLocation = inputJsonFileLocation;
		this.inputConfigFile = inputConfigFile;
		this.outputReportFileLocation = outputReportFileLocation;
	}

	public static void main(String[] args) throws InvalidNumberOfArgumentsException, MyCustomException, IOException {
		logger.info("started at " + LocalTime.now());
		long start = System.currentTimeMillis();
		if(null != args && args.length == 3) {
			int length = args.length;
			logger.info("Input Params : " + length);
			
			AvroFileOperations avroFileOperations = new AvroFileOperations(args[0], args[1], args[2]);
			CustomerModel customerModel = avroFileOperations.readJsonFile();
			logger.info("Total Customers : " + customerModel.getCustomers().size());

			avroFileOperations.createAvroFile(customerModel);
			// We can read our generated avro file
			ArrayList<Customer> validCustomers = new ArrayList<>();
			ArrayList<Customer> inValidCustomers = new ArrayList<>();
			List<ErrorRecord> errorCustomers = null;
			errorCustomers = avroFileOperations.validateRecordsCheck(validCustomers, inValidCustomers);

			if (!validCustomers.isEmpty())
				avroFileOperations.createValidDataPdf(validCustomers, "ValidCustomers");
			if (!inValidCustomers.isEmpty())
				avroFileOperations.createInvalidDataPdf(errorCustomers, "InvalidCustomers");
			
			logger.info("Report Generated successfully.....");
			logger.info("Total Time Taken : " + (System.currentTimeMillis() - start) / 1000 + " secs");
		} else  {
			throw new InvalidNumberOfArgumentsException("Need to pass 3 arguments");
		}
	}
	
	public CustomerModel readJsonFile() throws MyCustomException {
		CustomerModel customerModel = null;
		try {
			ObjectMapper mapper = new ObjectMapper();
			// Read the json file and put into object
			customerModel = mapper.readValue(new File(inputJsonFileLocation), CustomerModel.class);
		} catch (JsonParseException e) {
			logger.error(e.getMessage());
			throw new MyCustomException("JsonParsing Exception");
		} catch (JsonMappingException e1) {
			logger.error(e1.getMessage());
			throw new MyCustomException("JsonMapping Exception");
		} catch (IOException e2) {
			logger.error(e2.getMessage());
			throw new MyCustomException("File is not accessible");
		} finally {
			logger.info("successfully read the json file....");
		}
		return customerModel;
	}
	
	public void createAvroFile(CustomerModel customerModel) {
		Customer customer = null;
		ArrayList<Customer> customerList = new ArrayList<>();
		for (CustomerDetails cd : customerModel.getCustomers()) {
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
		// Write customers into avro file
		createAvroFile(customerList, customer, inputAvroFileLocation);
	}
	private static void createAvroFile(List<Customer> customerList, Customer customer, String inputAvroFileLocation) {
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
	}
	public List<ErrorRecord> validateRecordsCheck(List<Customer> validCustomers, List<Customer> inValidCustomers) throws IOException {

		DataFileReader<Customer> dataFileReaderGeneric = null;
		try {
			final File fileGeneric = new File(inputAvroFileLocation);
			final DatumReader<Customer> datumReaderGeneric = new SpecificDatumReader<>(Customer.class);
			dataFileReaderGeneric = new DataFileReader<>(fileGeneric, datumReaderGeneric);
			while (dataFileReaderGeneric.hasNext()) {
				Customer readCustomer = dataFileReaderGeneric.next();
				if (validateCustomer(readCustomer, inputConfigFile))
					validCustomers.add(readCustomer);
				else
					inValidCustomers.add(readCustomer);
			}

		} catch (IOException e) {
			logger.error(e.getMessage());
		} finally {
			if (dataFileReaderGeneric != null)
				dataFileReaderGeneric.close();
			logger.info("successfully read customer-output.avro");
		}
		return errorCustomers;
	}

	private static boolean validateCustomer(Customer customer, String inputConfigFile) {
		GetConfigValues configValues = new GetConfigValues();
		String regExp = configValues.getValueByKey("REG_EXP_NAMES", inputConfigFile);

		Predicate<String> namesCheck = cust -> Pattern.matches(regExp, cust);
		if (namesCheck.test(customer.getFirstName()) && namesCheck.test(customer.getLastName()))
			return true;
		else if (!namesCheck.test(customer.getFirstName()) && !namesCheck.test(customer.getLastName())) {
			ErrorRecord rec = new ErrorRecord();
			rec.setId(customer.getId());
			rec.setFirstNameFailed("FirstName & LastName fields not valid");
			errorCustomers.add(rec);
			return false;
		} else if (!namesCheck.test(customer.getFirstName())) {
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
			return false;
		}
	}
	public void createValidDataPdf(List<Customer> customers, String fileName) {
		try {
			Document document = new Document();
			PdfWriter.getInstance(document, new FileOutputStream(outputReportFileLocation + "/" + fileName + ".pdf"));

			document.open();
			PdfPTable table = null;

			String[] tableHeaders = { "Id", "First Name", "Last Name", "Age", "AutomatedEmail", "Height", "Weight" };
			table = new PdfPTable(tableHeaders.length);
			addTableHeader(table, tableHeaders);
			addRows(table, customers);
			
			document.add(table);
			document.close();
		} catch (FileNotFoundException e) {
			logger.error(e.getMessage());
		} catch (DocumentException e) {
			logger.error(e.getMessage());
		}
	}

	public void createInvalidDataPdf(List<ErrorRecord> errorCustomers, String fileName) {
		try {
			Document document = new Document();
			PdfWriter.getInstance(document, new FileOutputStream(outputReportFileLocation + "/" + fileName + ".pdf"));

			document.open();
			PdfPTable table = null;

			String[] tableHeaders = { "Id", "Error Reason" };
			table = new PdfPTable(tableHeaders.length);
			addErrorTableHeader(table, tableHeaders);
			addErrorRows(table, errorCustomers);
			
			document.add(table);
			document.close();
		} catch (FileNotFoundException e) {
			logger.error(e.getMessage());
		} catch (DocumentException e) {
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

	private static void addRows(PdfPTable table, List<Customer> customerList) {
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

	private static void addErrorRows(PdfPTable table, List<ErrorRecord> customerList) {
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