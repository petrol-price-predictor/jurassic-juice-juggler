## Environment

Since we are running python version 3.10.10 we need to install that
```BASH
pyenv install -v 3.10.10
```
We have to install hdf5:

```BASH
 brew install hdf5
 brew install graphviz
```
With the system setup like that, we can go and create our environment

There is a MAKE file available
```BASH
make
source .venv/bin/activate
```
alternatively:

```BASH
pyenv local 3.9.8
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install --no-binary=h5py h5py
pip install -r requirements.txt
```
If you already have hdf5
```BASH
export HDF5_DIR=/opt/homebrew/Cellar/hdf5/1.12.2
```
otherwise, if you have just installed hdf5 with brew, then
```BASH
export HDF5_DIR=/opt/homebrew/Cellar/hdf5/1.12.2_2
```
