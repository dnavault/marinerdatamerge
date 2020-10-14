import com.mariner.*;
import javax.swing.*;
import java.awt.*;
import java.io.*;
import java.sql.*;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;


import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import javax.swing.table.DefaultTableModel;
import javax.xml.parsers.*;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.*;
import java.sql.Timestamp;

public class GridReport {
    private JPanel reportpanel;

    public static void main(String[] args) throws IOException, SQLException, ParserConfigurationException, SAXException {

        Connection connection = DbConnection.getConnection();
        Statement statement = connection.createStatement();
        StringBuilder sql;

        String csvColumnHeaders = getColumnHeadersFromReportCSV();
        Report.setColumns(csvColumnHeaders);
        initReportsTable(connection, statement);

        List<Report> reportList = new LinkedList<Report>();
        // Load the CSV records
        loadCSVRecordsToReportList(reportList);
        // Load  JSON records
        loadJSONRecordsToReportList(reportList);
        // Load XML records
        loadXMLRecordsToReportList(reportList);
        JFrame frame = new JFrame();
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        // Display the summary of report counts grouped by service-guid
        ResultSet rsSummary = getServiceGuidSummaryResultset(connection, reportList);
        JTable summaryTable = new JTable(buildTableModel(rsSummary));

        outputDataMergeCSV(connection);

        JScrollPane scrollPane1 = new JScrollPane(summaryTable);
        frame.getContentPane().add(scrollPane1);
        //JScrollPane scrollPane = new JScrollPane(table);
        //frame.getContentPane().add(scrollPane);
        frame.pack();
        frame.setVisible(true);
    }

    /*
     Output the combined data from all sources
   */
    public static void outputDataMergeCSV(Connection connection) throws SQLException, IOException
    {
        Statement statement = connection.createStatement();
        String csvFilePath = "datamerge.csv";
        try {
            BufferedWriter fileWriter = new BufferedWriter(new FileWriter(csvFilePath));
            ResultSet rsDataMerge = statement.executeQuery("SELECT * FROM REPORTS WHERE [packets-serviced] > 0 ORDER BY [request-time] ASC;");
            // write header line containing column names
            fileWriter.write(String.join(",", Report.getColumns()));

            while (rsDataMerge.next()) {
                String clientAddress = rsDataMerge.getString("client-address");
                String clientGuid = rsDataMerge.getString("client-guid");
                String requestTime = rsDataMerge.getString("request-time");
                String serviceGuid = rsDataMerge.getString("service-guid");
                Integer retriesRequest = rsDataMerge.getInt("retries-request");
                Integer packetsRequested = rsDataMerge.getInt("packets-requested");
                Integer packetsServiced = rsDataMerge.getInt("packets-serviced");
                Integer maxHoleSize = rsDataMerge.getInt("max-hole-size");
                String line = String.format("\"%s\",%s,%s,%s,%s,%s,%s,%s",
                        clientAddress, clientGuid, requestTime, serviceGuid, retriesRequest, packetsRequested, packetsServiced, maxHoleSize);
                fileWriter.newLine();
                fileWriter.write(line);
            }
            statement.close();
            fileWriter.close();

        } catch (SQLException sqle) {
            System.out.println(sqle.getErrorCode());

        } finally {
            connection.close();
        }

    }

    /*
         Returns a ResultSet displaying the number of report counts grouped by service-guid column data
       */
    public static ResultSet getServiceGuidSummaryResultset(Connection connection, List<Report> reportList) throws SQLException {
        Statement statement = connection.createStatement();
        StringBuilder sql;
        for (int i = 0; i < reportList.size(); i++) {
            Report report = reportList.get(i);

            sql = new StringBuilder();
            sql.append("INSERT INTO REPORTS VALUES('");
            sql.append(report.getClient_address() + "','");
            sql.append(report.getClient_guid() + "','");
            sql.append(report.getRequest_time() + "','");
            sql.append(report.getService_guid() + "',");
            sql.append(report.getRetries_request());
            sql.append(",");
            sql.append(report.getPackets_requested());
            sql.append(",");
            sql.append(report.getPackets_serviced());
            sql.append(",");
            sql.append(report.getMax_hole_size());
            sql.append(");");

            statement.executeUpdate(sql.toString());
        }

        sql = new StringBuilder();
        sql.append("SELECT [service-guid], COUNT([service-guid]) AS Records FROM REPORTS GROUP BY [service-guid];");
        return statement.executeQuery(sql.toString());
    }

    /*
         Returns a table model used by the service-guid report count summary
    */
    public static DefaultTableModel buildTableModel(ResultSet rs)
            throws SQLException {

        ResultSetMetaData metaData = rs.getMetaData();

        // names of columns
        Vector<String> columnNames = new Vector<String>();
        int columnCount = metaData.getColumnCount();
        for (int column = 1; column <= columnCount; column++) {
            columnNames.add(metaData.getColumnName(column));
        }

        // data of the table
        Vector<Vector<Object>> data = new Vector<Vector<Object>>();
        while (rs.next()) {
            Vector<Object> vector = new Vector<Object>();
            for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
                vector.add(rs.getObject(columnIndex));
            }
            data.add(vector);
        }
        return new DefaultTableModel(data, columnNames);
    }

    /*
        Helper that returns the column headers in the reports.csv file
    */
    public static String getColumnHeadersFromReportCSV() throws FileNotFoundException {
        Scanner input = null;
        String csvHeaders;
        String filename = "./resources/reports.csv";
        try {

            File file = new File(filename);
            input = new Scanner(file);
            csvHeaders = input.nextLine();
        } catch (FileNotFoundException fnfe) {
            throw new FileNotFoundException(fnfe.getMessage());
        } finally {
            if (input != null) {
                input.close();
            }
        }
        return csvHeaders;
    }

    /*
     Appends to list of Report objects based on data in reports CSV file
   */
    public static void loadCSVRecordsToReportList(List<Report> reportList) throws FileNotFoundException {
        // CSV
        String filename = "./resources/reports.csv";
        File file = new File(filename);
        Scanner input = new Scanner(file);
        String line = input.nextLine();

        Report.setColumns(line);

        while (input.hasNextLine()) {
            line = input.nextLine();
            String[] arrValues = line.split(",");

            Report report = new Report();
            report.setClient_address(arrValues[0]);
            report.setClient_guid(arrValues[1]);
            report.setRequest_time(arrValues[2]);
            report.setService_guid(arrValues[3]);
            report.setRetries_request(Integer.parseInt(arrValues[4]));
            report.setPackets_requested(Integer.parseInt(arrValues[5]));
            report.setPackets_serviced(Integer.parseInt(arrValues[6]));
            report.setMax_hole_size(Integer.parseInt(arrValues[7]));
            reportList.add(report);
        }
        input.close();
    }

    /*
      Appends to list of Report objects based on data in reports XML file
    */
    public static void loadXMLRecordsToReportList(List<Report> reportList) throws ParserConfigurationException, SAXException, IOException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        DocumentBuilder builder = factory.newDocumentBuilder();
        File file = new File("./resources/reports.xml");
        Document doc = builder.parse(file);
        doc.getDocumentElement().normalize();
        // get the first element
        Element root = doc.getDocumentElement();
        // get all child nodes
        NodeList nodes = root.getChildNodes();
        // print the text content of each child
        for (int i = 0; i < nodes.getLength(); i++) {

            Node node = nodes.item(i);

            if (node.getNodeType() == Node.ELEMENT_NODE) {

                Element element = (Element) node;
                Report report = new Report();
                report.setClient_address(element.getElementsByTagName("client-address").item(0).getTextContent());
                report.setClient_guid(element.getElementsByTagName("client-guid").item(0).getTextContent());
                report.setRequest_time(element.getElementsByTagName("request-time").item(0).getTextContent());
                report.setService_guid(element.getElementsByTagName("service-guid").item(0).getTextContent());
                report.setRetries_request(Integer.parseInt(element.getElementsByTagName("retries-request").item(0).getTextContent()));
                report.setPackets_requested(Integer.parseInt(element.getElementsByTagName("packets-requested").item(0).getTextContent()));
                report.setPackets_serviced(Integer.parseInt(element.getElementsByTagName("packets-serviced").item(0).getTextContent()));
                report.setMax_hole_size(Integer.parseInt(element.getElementsByTagName("max-hole-size").item(0).getTextContent()));
                reportList.add(report);
            }
        }
    }

    /*
       Populates list of Report objects based on data in reports csv file
     */
    public static void loadJSONRecordsToReportList(List<Report> reportList)  {
        // JSON
        JSONParser jsonParser = new JSONParser();

        try {
            //Read JSON file
            FileReader reader = new FileReader("./resources/reports.json");
            Object obj = jsonParser.parse(reader);
            JSONArray reportJSON = (JSONArray) obj;

            for (int i = 0; i < reportJSON.size(); ++i) {
                Report report = new Report();
                JSONObject reportObj = (JSONObject) reportJSON.get(i);
                report.setClient_address((String) reportObj.get("client-address"));
                report.setClient_guid((String) reportObj.get("client-guid"));

                Timestamp ts = new Timestamp((Long) reportObj.get("request-time"));
                Date date = new Date(ts.getTime());
                String pattern = "yyyy-MM-dd HH:mm:ss zzz";
                // Create an instance of SimpleDateFormat used for formatting
                // the string representation of date according to the chosen pattern
                DateFormat df = new SimpleDateFormat(pattern);
                String dt = df.format(date);
                System.out.println(dt);
                report.setRequest_time(dt);

                report.setService_guid((String) reportObj.get("service-guid"));
                report.setRetries_request(Integer.parseInt(reportObj.get("retries-request").toString()));
                report.setPackets_requested(Integer.parseInt(reportObj.get("packets-requested").toString()));
                report.setPackets_serviced(Integer.parseInt(reportObj.get("packets-serviced").toString()));
                report.setMax_hole_size(Integer.parseInt(reportObj.get("max-hole-size").toString()));
                reportList.add(report);
            }
            System.out.println(reportJSON);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }

    /*
       Create the REPORTS table based on columns in reports.csv
     */
    public static void initReportsTable(Connection connection, Statement statement) {
        // String columnsSql = "[client-address] string,[client-guid] string,[request-time] string,[service-guid] string,"
        //        + "[retries-request] integer,[packets-requested] integer,[packets-serviced] integer,[max-hole-size] integer";

        String[] arrColumns = Report.getColumns();
        // client-address
        String clientAddress = arrColumns[0];
        clientAddress = "[" + clientAddress + "] string";
        // client-guid
        String clientGuid = arrColumns[1];
        clientGuid = "[" + clientGuid + "] string";
        // request-time
        String requestTime = arrColumns[2];
        requestTime = "[" + requestTime + "] string";
        // service-guid
        String serviceGuid = arrColumns[3];
        serviceGuid = "[" + serviceGuid + "] string";
        // retries-request
        String retriesRequest = arrColumns[4];
        retriesRequest = "[" + retriesRequest + "] integer";
        // packets-requested
        String packetsRequested = arrColumns[5];
        packetsRequested = "[" + packetsRequested + "] integer";
        // packets-serviced
        String packetsServiced = arrColumns[6];
        packetsServiced = "[" + packetsServiced + "] integer";
        // max-hole-size
        String maxHoleSize = arrColumns[7];
        maxHoleSize = "[" + maxHoleSize + "] integer";

        try {
            statement = connection.createStatement();
            statement.executeUpdate("DROP TABLE IF EXISTS REPORTS;");

            StringBuilder columnSql = new StringBuilder();
            columnSql.append(clientAddress + ",");
            columnSql.append(clientGuid + ",");
            columnSql.append(requestTime + ",");
            columnSql.append(serviceGuid + ",");
            columnSql.append(retriesRequest + ",");
            columnSql.append(packetsRequested + ",");
            columnSql.append(packetsServiced + ",");
            columnSql.append(maxHoleSize);
            statement.executeUpdate("CREATE TABLE REPORTS(" + columnSql + ");");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    private void createUIComponents() {
        // TODO: place custom component creation code here
    }

    {
// GUI initializer generated by IntelliJ IDEA GUI Designer
// >>> IMPORTANT!! <<<
// DO NOT EDIT OR ADD ANY CODE HERE!
        $$$setupUI$$$();
    }

    /**
     * Method generated by IntelliJ IDEA GUI Designer
     * >>> IMPORTANT!! <<<
     * DO NOT edit this method OR call it in your code!
     *
     * @noinspection ALL
     */
    private void $$$setupUI$$$() {
        createUIComponents();
        reportpanel.setLayout(new GridBagLayout());
        final JPanel spacer1 = new JPanel();
        GridBagConstraints gbc;
        gbc = new GridBagConstraints();
        gbc.gridx = 1;
        gbc.gridy = 0;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        reportpanel.add(spacer1, gbc);
        final JPanel spacer2 = new JPanel();
        gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 1;
        gbc.fill = GridBagConstraints.VERTICAL;
        reportpanel.add(spacer2, gbc);
    }

    /**
     * @noinspection ALL
     */
    public JComponent $$$getRootComponent$$$() {
        return reportpanel;
    }
}
