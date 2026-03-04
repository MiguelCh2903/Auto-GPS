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
collisions_folder: "output"
output_video_template: "collision_video_{id}.mp4"  # {id}, {id02d}, {topic}
camera_topics:
  - "/camera_left/image_raw/compressed"
gps_topic: "/swift/navsat_fix"
gps_tolerance_m: 50.0
padding_seconds: 2.0

extraction:
  mode: "fast"  # "fast" = sin re-encodear | "reencode" = H.264
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

Los videos se guardan en **la misma carpeta** donde está el archivo `collision_gps_XX.txt` de cada colisión.

Con `output_video_template: "collision_video_{id}.mp4"`:

```
output/
  collision_1/
    collision_gps_1.txt
    collision_video_1.mp4
  collision_2/
    collision_gps_2.txt
    collision_video_2.mp4
```

Si las carpetas tienen otro nombre (collision_01, col_001, etc), los videos van ahí igual.

## Modo `fast` vs `reencode`

- **fast**: extrae bytes JPEG crudos + FFmpeg stream copy → sin re-encodear, **muy rápido**, mejor calidad
- **reencode**: decodifica frames + re-codifica H.264 → lento, usa más CPU, útil si necesitas rectificación

## Requisitos

- Python 3.10+
- ffmpeg en PATH
- rosbag con topics: camera (compressed) + GPS (/swift/navsat_fix)
- collision_gps_XX.txt con formato estándar
