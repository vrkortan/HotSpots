//did not worry about free type (png writer has an optional dependancy on it)after messing with AWS
//  however you can install it with brew on a mac (worked for me)
//Compile:
//INHOUSE (on a MAC)
//g++ -Wall -O3 -Wno-deprecated `freetype-config --cflags` `freetype-config --libs` -DNO_FREETYPE -I/usr/local/include/ -I/opt/local/include -I/opt/local/include/freetype2 -o ./gausify gausify.cpp -L/usr/local/lib/ -L/opt/local/lib/ -lz -lpngwriter -lpng

//AWS
// to run the c++ code that was compiled on my mac on the linux cluster I had to:
    // apt-get install g++
    // apt-get install cmake
    // apt-get install libpng++dev
    // I tried to install freetype2 but never sucessfully got it in the correct place so the libpng package could find it.  Thus I just uncommented the "P_FREETYPE=1" line from the pnglib make.install.unix file
    // install libpng following directions from their website
    // use the below g++ command to compile
// g++ -std=c++11 -Wall -O3 -Wno-deprecated `freetype-config --cflags` `freetype-config --libs` -DNO_FREETYPE -I/usr/include/ -I/home/ubuntu/build/include -o ./gausify gausify.cpp -L/usr/include -L/home/ubuntu/build/lib -lz -lpngwriter -lpng

#include <cstring>
#include <iostream>
#include <string>
#include <fstream>
#include <math.h>
#include <string.h>
#include <sstream>
#include <complex>
#include <vector>
#include <stdio.h>
#include <ctime>
#include <pngwriter.h> //https://github.com/pngwriter/pngwriter
//#include <cstdlib> strtol(s.c_str(),0,10);

//from http://stackoverflow.com/a/236803
//to split a string by a delimter
std::vector<std::string> &split(const std::string &s, char delim, std::vector<std::string> &elems) {
    std::stringstream ss(s);
    std::string item;
    while (getline(ss, item, delim)) {
        elems.push_back(item);
    }
    return elems;
}
std::vector<std::string> split(const std::string &s, char delim) {
    std::vector<std::string> elems;
    split(s, delim, elems);
    return elems;
}
//

//change from grid coordinates to latitude/longitude
void grid_to_ll(double ll[2], int x, int y, double min_lon, double max_lon, double min_lat, double max_lat, double grid_x, double grid_y)
{
    //x is lon, y is lat
    //0,0 is MIN_LON, MIN_LAT
    double delta_lon = std::abs(max_lon-min_lon);
    double delta_lat = std::abs(max_lat-min_lat);
    double x_frac = (float)x / grid_x;
    double y_frac = (float)y / grid_y;
    double lon = min_lon + x_frac * delta_lon;
    double lat = min_lat + y_frac * delta_lat;

    ll[0] = lon;
    ll[1] = lat;
}

//change from latitude/longitude to grid coordinates
void ll_to_pixel(double xy[2], int lon, int lat, double min_lon, double max_lon, double min_lat, double max_lat, double grid_x, double grid_y)\
{
    //x is lon, y is lat
    //0,0 is MIN_LON, MIN_LAT
    double adj_lon = lon - min_lon;
    double adj_lat = lat - min_lat;
    double delta_lon = std::abs(max_lon - min_lon);
    double delta_lat = std::abs(max_lat - min_lat);
    double lon_frac = adj_lon / delta_lon;
    double lat_frac = adj_lat / delta_lat;

    xy[0] = int(lon_frac * grid_x);
    xy[1] = int(lat_frac * grid_y);
}



int main (int argc, char* argv[])
{
    //17 colors for png that correspond to colors in app, blue to red
    double colors_highred[18][3] =
        {{0./225., 0./225., 225./225.}, //blue
        {0./225., 86./225., 225./225.},
        {0./225., 127./225., 225./225.},
        {0./225., 171./225., 225./225.},
        {0./225., 213./225., 225./225.},
        {0./225., 240./225., 225./225.},
        {0./225., 225./225., 225./225.},
        {0./225., 225./225., 0./225.},
        {128./225., 225./225., 0./225.},
        {176./225., 225./225., 0./225.},
        {218./225., 225./225., 0./225.},
        {225./225., 225./225., 0./225.},
        {225./225., 240./225., 0./225.},
        {225./225., 208./225., 0./225.},
        {225./225., 171./225., 0./225.},
        {225./225., 127./225., 0./225.},
        {225./225., 91./225., 0./225.},
        {225./225., 0./225., 0./225.}}; //red

    //17 colors for png that correspond to colors in app, red to blue
    double colors_highblue[18][3] =
        {{225./225., 0./225., 0./225.}, //red
        {225./225., 91./225., 0./225.},
        {225./225., 127./225., 0./225.},
        {225./225., 171./225., 0./225.},
        {225./225., 208./225., 0./225.},
        {225./225., 240./225., 0./225.},
        {225./225., 225./225., 0./225.},
        {218./225., 225./225., 0./225.},
        {176./225., 225./225., 0./225.},
        {128./225., 225./225., 0./225.},
        {0./225., 225./225., 0./225.},
        {0./225., 225./225., 225./225.},
        {0./225., 240./225., 225./225.},
        {0./225., 213./225., 225./225.},
        {0./225., 171./225., 225./225.},
        {0./225., 127./225., 225./225.},
        {0./225., 86./225., 225./225.},
        {0./225., 0./225., 225./225.}}; //blue

    int Narg = 1;
    int i;

	//Argument/parameter initialization
    std::string filename;
    double min_lon = 0.0;
    double max_lon = 0.0;
    double min_lat = 0.0;
    double max_lat = 0.0;
    double min_target = 0.0;
    double max_target = 0.0;
    int grid_x = 500;
    int grid_y = 500;
    double sigma = 0.05;
    int distance_cutoff = 10; //will be in factors of sigma
    int highred = 1; //will say if the color scale should be, high=red=1, high=blue=0
    // double devisions[18];
    int blocks = 5;
    //read in command line arguments
	for (i=1; i<argc; i++)
	{
        if (strncmp(argv[i],"-l=",3)==0)
        {
            if (strchr(argv[i],',')!=NULL)
            {
                sscanf(argv[i],"-l=%lf,%lf,%lf,%lf",&min_lon,&max_lon,&min_lat,&max_lat);
            }
            Narg++;
        }
        if (strncmp(argv[i],"-g=",3)==0)
        {
            if (strchr(argv[i],',')!=NULL)
            {
                sscanf(argv[i],"-g=%i,%i",&grid_x,&grid_y);
            }
            Narg++;
        }
        if (strncmp(argv[i],"-t=",3)==0)
        {
            if (strchr(argv[i],',')!=NULL)
            {
                sscanf(argv[i],"-t=%lf,%lf",&min_target,&max_target);
            }
            Narg++;
        }
        if (strncmp(argv[i],"-s=",3)==0)
        {
            sscanf(argv[i],"-s=%lf",&sigma);
            Narg++;
        }
        if (strncmp(argv[i],"-b=",3)==0)
        {
            sscanf(argv[i],"-b=%i",&blocks);
            Narg++;
        }
        if (strncmp(argv[i],"-c=",3)==0)
        {
            sscanf(argv[i],"-c=%i",&distance_cutoff);
            Narg++;
        }
        if (strncmp(argv[i],"-i=",3)==0)
        {
            sscanf(argv[i],"-i=%i",&highred);
            Narg++;
        }
	}

    //usage
	if (argc<(1+Narg))
	{
		std::cout << "Usage: gausify -l=min_lon,max_lon,min_lat,max_lat -g=grid_x,grid_y -s=sigma -b=blocks -c=distance_cutoff filename -i=1" << std::endl;
		std::cout << "convolutes data with a Gaussian and writes to .png file" << std::endl;
		return 1;
	}

    filename = argv[Narg];

    //check to make sure output file does not already exist
    std::stringstream ss;
    std::string output_filename;
    std::vector<std::string> filename_vector;
    std::vector<std::string> date_vector;
    char d1 = '/';
    char d2 = '.';
    filename_vector = split(filename, d1);
    std::string date_tmp = filename_vector.back();
    date_vector = split(date_tmp,d2);
    std::string date = date_vector[0];
    filename_vector.pop_back();
    for(int i = 0; i < filename_vector.size(); i++){
        ss << filename_vector[i] << "/";
    }
    ss << grid_x << "X" << grid_y << "_sigmablocks_" << blocks << "/" << date << ".png";

    output_filename = ss.str();
    ss.str("");
    if (std::ifstream(output_filename.c_str())){
        std::cerr << "\nOutput file exists : " << output_filename.c_str() << '\n';
        exit(0);
    }

    //read flip stats by month csv file into vector (value, lon, lat)
    std::vector<std::vector<double> > values;
    std::ifstream f_in(filename.c_str());
    if (!f_in) {
        std::cerr << "\nCan't open input file " << filename << '\n';
        exit(0);
    } else {
        while (f_in)
        {
            std::string s;
            if (!getline(f_in, s)){
                break;
            }
            //std::istream ss(s);
            std::istringstream ss(s);
            std::vector <double> record;
            while(ss)
            {
                std::string s;
                if (!getline( ss, s, ',' )){
                    break;
                }
                record.push_back( std::stod(s) );
            }
            //remove last values (sold_price_A, beds_A, beds_B, baths_A, baths_B, change_finished_square_feet), not necessary for the plot
            for(int j = 0; j < 6; j++){
                record.pop_back();
            }

            values.push_back( record );
        }
    }

    //prepair to write png file
    double bucket_size = std::abs(max_target - min_target) / 18.;
    double sum = 0.0;

    FILE *f_out;
    f_out = fopen(output_filename.c_str(),"w");

    if (!f_out) {
        std::cerr << "\nCan't open output file " << output_filename << '\n';
        exit(0);
    } else {

        pngwriter png(grid_x, grid_y, 1.0, output_filename.c_str());

        //gaussian convolution
        for (int yi=0; yi<grid_y; yi++)
        {
            for (int xi=0; xi<grid_x; xi++)
            {
                sum = 0.0;

                //turn grid to lat/lon
                double ll[2];
                grid_to_ll(ll, xi, yi, min_lon, max_lon, min_lat, max_lat, grid_x, grid_y);
                double pt_lon = ll[0];
                double pt_lat = ll[1];

                //compute weighted values
                for (i=0; i<values.size(); i++)
                {
                    double temp_val = values[i][0];
                    double temp_lon = values[i][1];
                    double temp_lat = values[i][2];
                    double d = sqrt( pow(pt_lon - temp_lon, 2) + pow(pt_lat - temp_lat, 2) );
                    if(d <= distance_cutoff * sigma){
                        sum += temp_val * exp( -1. * pow(d / (sigma * sqrt(2.)),2) );
                    }
                }

                //set if use red as the high value or blue as the high value
                double colors[18][3];
                if(highred == 0){
                    //std::copy(std::begin(colors_highblue), std::end(colors_highblue), std::begin(colors));
                    for(int i=0; i<18; i++){
                        for(int k=0; k<3; k++){
                            colors[i][k] = colors_highblue[i][k];
                        }
                    }
                } else {
                    //std::copy(std::begin(colors_highred), std::end(colors_highred), std::begin(colors));
                    for(int i=0; i<18; i++){
                        for(int k=0; k<3; k++){
                            colors[i][k] = colors_highred[i][k];
                        }
                    }
                }

                //turn sum into color
                double r;
                double g;
                double b;
                double tmp = min_target;
                int idx = 0;
                if (sum > tmp){
                    while (sum > tmp){
                        tmp += bucket_size;
                        idx += 1;
                    }
                }
                if (idx >= 17){
                    r = colors[17][0];
                    g = colors[17][1];
                    b = colors[17][2];
                } else {
                    r = colors[idx][0];
                    g = colors[idx][1];
                    b = colors[idx][2];
                }

                //plot point
                png.plot(xi, yi, r, g, b);
            }
        }

        png.close();
    }
}
