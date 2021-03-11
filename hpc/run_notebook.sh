#!/bin/bash
#SBATCH -p batch
#SBATCH -N 1
#SBATCH -n 1
#SBATCH --time=12:30:00
#SBATCH --mem=64GB
#SBATCH --err="ramnb.info"
#SBATCH --output="ramnb.info"
#SBATCH --job-name="ramnb"

# Setup Python Environment
module load Singularity
module load CUDA/10.2.89

# get tunneling info
port=$(shuf -i8000-9999 -n1)
node=$(hostname -s)
user=$(whoami)
cluster=$(hostname -f | awk -F"." '{print $2}')

# print tunneling instructions
echo "Paste this command in your terminal."
echo "ssh -N -L ${port}:${node}:${port} -L 6006:${node}:6006 ${user}@${cluster}phoenix-login1.adelaide.edu.au"

# Start singularity instance
singularity exec main.simg jupyter notebook --port=${port} --ip=${node}
