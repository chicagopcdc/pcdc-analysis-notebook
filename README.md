# RStudio Environment for AVRO File Analysis

## Prerequisites

- [Docker](https://www.docker.com/) installed on your system

---

##  How to Use

### Step 1: Clone the Repository

```bash
git clone https://github.com/chicagopcdc/pcdc-analysis-notebook.git
cd pcdc-analysis-notebook
```

### Step 2: Build the Docker Image

```bash
docker build -t my-rstudio .
```

### Step 3: Run the Container

```bash
docker run -d --name my-rstudio-container -p 8787:8787 -e DISABLE_AUTH=true my-rstudio
```

#### Access the rstudio interface

Open your browser and visit:

```
http://localhost:8787
```

You will find the `analyze_avro.R` file available in `/home/rstudio`.

If not, you can manually copy and paste the code from the repository into a new R script.


### Step 4: Upload the AVRO File

1. In RStudio's Files pane, click Upload and select your `.avro` file.
2. Once uploaded, note the full path, usually:

```bash
/home/rstudio/your_file.avro
```
3. Update your R script to reference the uploaded file:

```bash
# Replace 'your_file.avro' with your actual filename
avro_file_path <- "/home/rstudio/your_file.avro"
```
And then you can perform operations on the file using your analysis code.

---

### 💡 Tips

You can **stop** the container:

```bash
docker stop my-rstudio-container
```

And start it again later without rebuilding:

```bash
docker start my-rstudio-container
```