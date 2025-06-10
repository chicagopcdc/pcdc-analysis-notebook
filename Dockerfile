FROM jupyter/base-notebook:python-3.10

# Install required Python packages
RUN pip install --no-cache-dir pypfb pandas papermill

# working directory inside container to resources folder
WORKDIR /home/jovyan/resources

CMD ["bash", "-c", "papermill PFB.ipynb PFB.ipynb && start-notebook.sh --NotebookApp.token='' --NotebookApp.password='' --NotebookApp.notebook_dir=/home/jovyan/resources"]