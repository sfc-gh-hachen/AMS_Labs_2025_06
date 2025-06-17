# Analytics Platform Concurrency Test

This repository contains the necessary configuration files to run a concurrency test on your analytics platform. This test will help you understand how your platform performs under load, which is a critical factor in ensuring a good user experience.

This guide is intended for **Windows users**.

---

## Why is it so important?

Choosing an analytics platform is a big decision. While many platforms showcase impressive performance with a single user, the real test comes when multiple users are accessing the platform simultaneously. A platform that seems fast with one user can become slow and unresponsive with 10, 50, or 100+ concurrent users. This is because each additional user consumes resources, and if the platform isn't built to handle this, performance will degrade for everyone.

This simple test will allow you to simulate a multi-user environment and measure your platform's performance under stress. By doing so, you can make a more informed decision and choose a platform that will scale with your needs.

---

## Prerequisites (Windows)

You will need to download and install the following tools to run this test:

* **Java Development Kit (JDK):** JMeter is a Java-based application, so you'll need to have the JDK installed. If you don't have it, you can download it from the official Oracle website.
    * [Download Java](https://www.oracle.com/java/technologies/downloads/)
* **Apache JMeter:** This is the open-source tool we'll be using to run the concurrency test.
    * [Download Apache JMeter](https://jmeter.apache.org/download_jmeter.cgi)
* **Snowflake JDBC Driver:** This driver is required for JMeter to connect to Snowflake. You should download the latest version.
    * [Download Snowflake JDBC Driver](https://developers.snowflake.com/drivers/)
* **Test Configuration Files:** These are included in this repository in the `Concurrency_Test_Config_Only.zip` file.

---

## Step-by-Step Guide

### 1. Download the Test Files

* Download the `Concurrency_Test_Config_Only.zip` file from this repository.
* Unzip the contents to a temporary folder on your computer. You will find the following files:
    * `Concurrency_Test_Snowflake_Public - Clean.jmx`
    * `jpgc-casutg-3.1.1.zip`
    * `SearchFilters.csv`

### 2. Install Java

* If you don't already have Java installed, go to the [Oracle Java download page](https://www.oracle.com/java/technologies/downloads/) and download the Windows installer.
* Run the installer and follow the on-screen instructions.

### 3. Download and Set Up JMeter

* Go to the [Apache JMeter download page](https://jmeter.apache.org/download_jmeter.cgi).
* Under the "Binaries" section, download the `.zip` file (e.g., `apache-jmeter-5.x.zip`).
* Unzip the downloaded file to a new folder. This will be your JMeter installation directory (e.g., `C:\apache-jmeter-5.6.3`).

### 4. Configure JMeter Folders

This is the most critical step. You need to place the test files and drivers in the correct folders within your JMeter installation directory.

1.  **Download the Snowflake JDBC Driver:** Go to the [Snowflake JDBC download page](https://developers.snowflake.com/drivers/) and download the latest JAR file (e.g., `snowflake-jdbc-3.14.2.jar`).
2.  **Place the JDBC Driver:** Copy the downloaded Snowflake JDBC `.jar` file into the **`lib`** folder of your JMeter directory.
    * `C:\apache-jmeter-5.6.3\`**`lib`**`\snowflake-jdbc-3.14.2.jar`
3.  **Place the JMeter Plugins:**
    * Unzip the `jpgc-casutg-3.1.1.zip` file you downloaded from this repository.
    * Copy `jmeter-plugins-cmn-jmeter-0.7.jar` into the **`lib`** folder.
    * Copy `jmeter-plugins-casutg-3.1.1.jar` into the **`lib/ext`** folder.
    * Copy `jmeter-plugins-manager-1.10.jar` into the **`lib/ext`** folder.
4.  **Place the Test Plan and CSV:**
    * Copy `Concurrency_Test_Snowflake_Public - Clean.jmx` into the **`bin`** folder.
    * Copy `SearchFilters.csv` into the **`bin`** folder.

### 5. Launch JMeter

* Open the **`bin`** folder in your JMeter installation directory.
* Double-click on the `jmeter.bat` file to launch JMeter.

### 6. Open the Test Plan

* In JMeter, click on `File > Open`.
* Since you are already in the `bin` directory, the `Concurrency_Test_Snowflake_Public - Clean.jmx` file should be visible. Select it and click "Open".

### 7. Run the Test

* In the left-hand pane of the JMeter window, you will see the test plan tree.
* Click the green "Start" button in the toolbar to begin the test.

### 8. Analyze the Results

* As the test runs, you can view the results in real-time by clicking on the different "Listeners" in the test plan tree (e.g., "View Results Tree", "Summary Report").
* These listeners will show you important metrics like the average response time, error rate, and throughput, which will help you assess your platform's performance under concurrent load.
