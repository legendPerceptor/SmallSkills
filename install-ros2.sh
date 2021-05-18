locale  # check for UTF-8

sudo apt update && sudo apt install locales
sudo locale-gen en_US en_US.UTF-8
sudo update-locale LC_ALL=en_US.UTF-8 LANG=en_US.UTF-8
export LANG=en_US.UTF-8

locale  # verify settings
#
sudo apt update && sudo apt install curl gnupg2 lsb-release
sudo curl -sSL https://raw.githubusercontent.com/ros/rosdistro/master/ros.key  -o /usr/share/keyrings/ros-archive-keyring.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/ros-archive-keyring.gpg] http://packages.ros.org/ros2/ubuntu $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/ros2.list > /dev/null
#
sudo apt update
sudo apt install ros-foxy-desktop
sudo apt install ros-foxy-ros-base
source /opt/ros/foxy/setup.bash
#

sudo apt install -y python3-argcomplete
sudo apt install gazebo11 ros-foxy-gazebo-ros-pkgs
sudo apt install ros-foxy-cartographer
sudo apt install ros-foxy-cartographer-ros
sudo apt install ros-foxy-navigation2
sudo apt install ros-foxy-nav2-bringup 
sudo apt install python3-vcstool
#
sudo apt install python3-colcon-common-extensions

# this installed the webot demos
# this needs to be run on the console or at least with X

sudo apt-get install ros-foxy-webots-ros2
source /opt/ros/foxy/local_setup.bash
ros2 launch webots_ros2_demos armed_robots.launch.py
