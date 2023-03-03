# -*- coding:utf-8 -*- 
# author = 'denishuang'
from __future__ import unicode_literals
import subprocess

FFMPEG = "ffmpeg"


class Transformer(object):

    def __init__(self, cmd='ffmpeg'):
        self.cmd = cmd

    def execute(self, *args, **kwargs):
        r = subprocess.run([self.cmd, '-y', '-loglevel', 'error'] + list(args), **kwargs)
        if r.returncode:
            raise Exception(r.stderr)
        return r.stdout

    def probe(self, *args, **kwargs):
        return subprocess.run([self.cmd.replace('ffmpeg', 'ffprobe')] + list(args), **kwargs)

    def save(self, src_path, dest_path, *args, **kwargs):
        return self.execute('-i', src_path, dest_path, *args, **kwargs)

    def video_to_images(self, video_path, output_dir, fps=6, file_name_template='%05d.png', **kwargs):
        return self.execute('-i', video_path, '-r', f'{fps}', '-f', 'image2', f'{output_dir}/{file_name_template}',
                            **kwargs)

    def video_set_audio(self, video_path, audio_path, save_path, **kwargs):
        return self.execute('-i', video_path, '-i', audio_path, '-map', '0:v', '-map', '1:a', '-c:v', 'copy', save_path,
                            **kwargs)

    def images_to_video(self, images_path, video_path, fps=5, **kwargs):
        return self.execute('-r', f'{fps}', '-i', images_path, '-pix_fmt', 'yuv420p', '-vf',
                            "pad=ceil(iw/2)*2:ceil(ih/2)*2", video_path, **kwargs)

    def video_concat(self, videos, output, **kwargs):
        inputs = []
        streams = ''
        n = len(videos)
        for i, v in enumerate(videos):
            streams += f'[{i}:0][{i}:1]'
            inputs.append('-i')
            inputs.append(v)

        return self.execute(*inputs, '-filter_complex', f'{streams}concat=n={n}:v=1:a=1[v][a]', '-map', '[v]', '-map',
                            '[a]', output, **kwargs)
