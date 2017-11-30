#/bin/bash
wget http://35.177.96.93/fs.tar.xz
mkdir  ~/Documents/fs
tar -xf fs.tar.xz -C  ~/Documents/fs
sudo chmod -R 777 ~/Documents/fs/
sudo docker pull aminiosa/dsns1:latest
sudo docker run -it -v ~/Documents/fs:/var/lib/mysql/ aminiosa/dsns1 









