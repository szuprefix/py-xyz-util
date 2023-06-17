# -*- coding:utf-8 -*- 
# author = 'denishuang'
from __future__ import unicode_literals
import subprocess

FFMPEG = "ffmpeg"


class Transformer(object):

    def __init__(self, cmd='ffmpeg'):
        self.cmd = cmd

    def execute(self, *args, **kwargs):
        cmd_params = [self.cmd, '-y', '-loglevel', 'error'] + list(args)
        r = subprocess.run(cmd_params, **kwargs)
        if r.returncode:
            error = r.stderr.decode() if r.stderr else ''
            raise Exception('%s:%s\n%s' % (r.returncode, error, cmd_params))
        return r.stdout

    def probe(self, *args, **kwargs):
        from .crawlutils import extract_between
        rs = subprocess.run([self.cmd.replace('ffmpeg', 'ffprobe')] + list(args), capture_output=True, **kwargs)
        s = rs.stderr.decode()
        ss = [l for l in s.split('\n') if 'Stream #' in l]
        d = {'duration': extract_between(s, 'Duration: ', ', ')}
        for stream in ss:
            if ' Video:' in stream:
                sts = stream.split(', ')
                for st in sts:
                    ps = st.split(' ')
                    if ps[-1] == 'fps':
                        d['fps'] = int(float(ps[0]))
                    elif '[SAR' in st:
                        w, h = ps[0].split('x')
                        d['w'], d['h'] = int(w), int(h)
                if 'w' not in d and 'x' in sts[2]:
                    w, h = sts[2].strip().split(' ')[0].split('x')
                    d['w'], d['h'] = int(w), int(h)

        return d

    def save(self, src_path, *args, **kwargs):
        return self.execute('-i', src_path, *args, **kwargs)

    def crop(self, src_path, dest_path, w=0, h=0, x=0, y=0, **kwargs):
        return self.save(src_path, '-vf', f'crop={w}:{h}:{x}:{y}', dest_path, **kwargs)

    def video_to_images(self, video_path, output_dir, fps=6, file_name_template='%05d.png', **kwargs):
        return self.execute('-i', video_path, '-r', f'{fps}', '-f', 'image2', f'{output_dir}/{file_name_template}',
                            **kwargs)

    def video_set_audio(self, video_path, audio_path, save_path, *args, **kwargs):
        return self.execute('-i', video_path, '-i', audio_path, '-map', '0:v', '-map', '1:a', '-c:v', 'copy', *args, save_path,
                            **kwargs)

    def images_to_video(self, images_path, video_path, *args, vf='', fps=5, **kwargs):
        if isinstance(images_path, (list, tuple)) and images_path:
            from .cmdutils import link_sequenced_files
            from pathlib import Path
            ext = Path(images_path[0]).suffix
            dir = link_sequenced_files(images_path, ext=ext)
            images_path = f'{dir.name}/%05d{ext}'
        return self.execute('-r', f'{fps}', '-i', images_path, '-pix_fmt', 'yuv420p', '-vf',
                            f"pad=ceil(iw/2)*2:ceil(ih/2)*2{vf}", *args, video_path, **kwargs)

    def video_concat(self, videos, output, audio=1, vf='', **kwargs):
        inputs = []
        streams = ''
        n = len(videos)
        for i, v in enumerate(videos):
            streams += f'[{i}:0]'
            if audio is not None:
                streams += f'[{i}:{audio}]'
            inputs.append('-i')
            inputs.append(v)

        if audio is None:
            return self.execute(*inputs, '-filter_complex', f'{streams}concat=n={n}:v=1[v]{vf}', '-map', '[v]', output, **kwargs)
        return self.execute(*inputs, '-filter_complex', f'{streams}concat=n={n}:v=1:a=1[v][a]', '-map', '[v]', '-map',
                            '[a]', output, **kwargs)
