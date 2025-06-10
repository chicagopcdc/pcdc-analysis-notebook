# PFB File QA/QC Checker and Visualizer

> A Dockerized Jupyter Notebook environment to perform quality checks and visualize Portable Format for Bioinformatics (`.avro`) files.

---

## 📁 Project Structure

```
pcdc-analysis-notebook/
├── Dockerfile
├── README.md
└── resources/
    ├── PFB.ipynb
    └── *.avro  # Add your own AVRO files here
```

---

## Prerequisites

- [Docker](https://www.docker.com/) installed on your system

---

##  How to Use

### Step 1: Clone the Repository

```bash
git clone https://github.com/chicagopcdc/pcdc-analysis-notebook.git
cd pcdc-analysis-notebook
```

### Step 2: Add Your `.avro` File

Copy the `.avro` file you want to visualize into the `resources/` directory.

Example:

```
resources/
├── PFB.ipynb
└── mydata_2025.avro
```

### Step 3: Build the Docker Image

```bash
docker build -t pfb-notebook .
```

### Step 4: Run the Container

> Replace `your_file.avro` with your actual file name inside the `resources/` directory.

#### Windows (CMD)

```cmd
docker run -p 8888:8888 -v "%cd%\resources:/home/jovyan/resources" -e AVRO_FILE_PATH="your_file.avro" pfb-notebook
```

#### Windows (PowerShell)

```powershell
docker run -p 8888:8888 -v ${PWD}/resources:/home/jovyan/resources -e AVRO_FILE_PATH="your_file.avro" pfb-notebook
```

#### Linux / macOS (Bash)

```bash
docker run -p 8888:8888 -v "$(pwd)/resources":/home/jovyan/resources -e AVRO_FILE_PATH="your_file.avro" pfb-notebook
```

---

## Access the Jupyter Notebook

Open your browser and visit:

```
http://localhost:8888
```

It will open directly into the `/home/jovyan/resources` folder with no token/password required.

---

## Notes

- Make sure your AVRO file path matches exactly what you pass in `AVRO_FILE_PATH`.
- The notebook `PFB.ipynb` reads this value to load the data.
- The AVRO file must be in the **Portable Format for Bioinformatics (PFB)** format for proper processing and visualization.