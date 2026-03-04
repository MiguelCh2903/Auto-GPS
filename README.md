# Auto-GPS

Extrae segmentos de video de rosbags .mcap según coordenadas GPS de colisiones.

## Instalación

```bash
cd Auto-GPS
pip install -e .
ffmpeg --version  # asegúrate de tener ffmpeg instalado
```

## Configuración

Edita `config.yaml`:

```yaml
rosbag_path: "path/to/recording.mcap"
collisions_folder: "output"  # donde están los collision_XX/collision_gps_XX.txt
output_folder: "output/videos"
camera_topics:
  - "/camera_left/image_raw/compressed"
gps_topic: "/swift/navsat_fix"
gps_tolerance_m: 50.0
padding_seconds: 2.0

extraction:
  mode: "fast"  # "fast" = sin re-encodear (rápido) | "reencode" = H.264 (lento)
```

## Uso

```bash
# Ver topics del rosbag
python -m autogps info rosbags/recording.mcap

# Ver colisiones detectadas (sin procesar)
python -m autogps parse-collisions output/

# Procesar todo
python -m autogps run --config config.yaml

# Con overrides
python -m autogps run --config config.yaml --mode fast --rosbag ruta/al/mcap
```

## Output

```
output/
  videos/
    collision_01/
      collision_01_camera_left_image_raw_compressed.mp4
    collision_02/
      ...
```

## Modo `fast` vs `reencode`

- **fast**: extrae bytes JPEG crudos + FFmpeg stream copy → sin re-encodear, **muy rápido**, mejor calidad
- **reencode**: decodifica frames + re-codifica H.264 → lento, usa más CPU, útil si necesitas rectificación

## Requisitos

- Python 3.10+
- ffmpeg en PATH
- rosbag con topics: camera (compressed) + GPS (/swift/navsat_fix)
- collision_gps_XX.txt con formato estándar
