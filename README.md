# Reduct C++ Example with PlotJuggler Demo Data

This repo shows how to use the Reduct C++ SDK to browse a bucket and query data
(CSV, JSON, ROS/MCAP, images, point clouds) against ReductStore.

The same examples are also available in Python under `examples/ros_demo.ipynb`.

## Build

```bash
cmake -S . -B build
cmake --build build -j
```

Executables will be placed in `build/bin/`.

## Run

```bash
./build/buckets_browse
./build/extract_csv_accx_gt10
./build/extract_json_accz_lt_neg5
./build/extract_mcap_ros_topic
./build/extract_images_save
./build/extract_pointcloud2
```

Each example connects to the public demo bucket:

* URL: `https://test.reduct.store`
* TOKEN: `plot_juggler_demo-3af058eb003b2ebd9a0d6f4fe899702f`
* BUCKET: `plot_juggler_demo`

To use your own instance, edit the constants at the top of each `.cc` file in `src/`.