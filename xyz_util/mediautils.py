# -*- coding:utf-8 -*- 
# author = 'denishuang'
from __future__ import unicode_literals
import subprocess

FFMPEG = "ffmpeg"


class Transformr(object):

    def __init__(self, cmd='ffmpeg'):
        self.cmd = cmd

    def execute(self, *args, **kwargs):
        return subprocess.run([self.cmd, '-y'] + list(args), **kwargs)

    def probe(self, *args, **kwargs):
        return subprocess.run([self.cmd.replace('ffmpeg', 'ffprobe')] + list(args), **kwargs)

    def save(self, src_path, dest_path):
        return self.execute('-i', src_path, dest_path)

    def video_to_images(self, video_path, output_dir, fps=5, file_name_template='%05d.png'):
        return self.execute('-i', video_path, '-r', f'{fps}', '-f', 'image2', f'{output_dir}/{file_name_template}')

    def video_set_audio(self, video_path, audio_path, save_path):
        return self.execute('-i', video_path, '-i', audio_path, '-map', '0:v', '-map', '1:a', '-c:v', 'copy', save_path)

    def images_to_video(images_path, video_path, fps=5):
        return subprocess.run([FFMPEG, '-y', '-r', f'{fps}', '-i', images_path, '-pix_fmt', 'yuv420p', '-vf',
                               "pad=ceil(iw/2)*2:ceil(ih/2)*2", video_path])
