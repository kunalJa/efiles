"""Manual runner for the production image pipeline in lambda_order_processor.py.

This file is not imported by the Lambda and is not part of the Lambda deployment.
It exists only to render and inspect images locally or exercise the S3 pipeline.

Usage:
    uv run python generate_printful_images.py
    LOCAL_PDF=/path/to/test.pdf SKIP_UPLOAD=1 uv run python generate_printful_images.py
"""

import json
import os
from io import BytesIO

from lambda_order_processor import (
    file_id_from_key,
    generate_printful_images,
    render_back_image,
    render_front_image,
)


def load_local_env() -> None:
    from dotenv import load_dotenv

    env_path = os.path.join(os.path.dirname(__file__), '..', '.env.development')
    if os.path.isfile(env_path):
        load_dotenv(env_path)
    elif os.path.isfile('.env.development'):
        load_dotenv('.env.development')


def main() -> None:
    load_local_env()
    bucket = os.environ.get('AWS_S3_BUCKET_NAME', 'kz-pdf-files-bucket')
    pdf_s3_key = os.environ.get('PDF_S3_KEY', 'VOL00001/EFTA00000001.pdf')
    asset_base_url = os.environ.get('PRINTFUL_ASSET_BASE_URL')
    local_pdf = os.environ.get('LOCAL_PDF')
    skip_upload = os.environ.get('SKIP_UPLOAD')

    if local_pdf or skip_upload:
        file_id = file_id_from_key(local_pdf or pdf_s3_key)
        if local_pdf:
            with open(local_pdf, 'rb') as pdf_file:
                pdf_buffer = BytesIO(pdf_file.read())
        else:
            import boto3

            s3_client = boto3.client('s3')
            pdf_buffer = BytesIO()
            s3_client.download_fileobj(bucket, pdf_s3_key, pdf_buffer)
            pdf_buffer.seek(0)

        images = {
            'front': render_front_image(pdf_buffer),
            'back': render_back_image(file_id),
        }
        os.makedirs('out', exist_ok=True)
        for name, image in images.items():
            path = f'out/{file_id}_{name}.png'
            with open(path, 'wb') as output_file:
                output_file.write(image.read())
            print(f'Wrote {path}')
        return

    import boto3

    result = generate_printful_images(
        boto3.client('s3'), bucket, pdf_s3_key, asset_base_url)
    print(json.dumps({
        key: value for key, value in result.items()
        if key.endswith('_key') or key == 'file_id'
    }, indent=2))
    print(f"Front URL: {result['front_url']}")
    print(f"Back URL: {result['back_url']}")


if __name__ == '__main__':
    main()
