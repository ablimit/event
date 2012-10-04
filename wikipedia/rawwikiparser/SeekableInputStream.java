/**
 * Copyright 2011 Yusuke Matsubara
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.FilterInputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;

//hello
public class SeekableInputStream extends FilterInputStream implements Seekable {
  private final Seekable seek;
  public SeekableInputStream(FSDataInputStream in) {
    super(in);
    this.seek = in;

  }

  public static SeekableInputStream getInstance(Path path, long start, long end, FileSystem fs, CompressionCodecFactory compressionCodecs) throws IOException {
    FSDataInputStream din = fs.open(path);
      din.seek(start);
      return new SeekableInputStream(din);
  }
  public static SeekableInputStream getInstance(FileSplit split, FileSystem fs, CompressionCodecFactory compressionCodecs) throws IOException {
    return getInstance(split.getPath(), split.getStart(), split.getStart() + split.getLength(), fs, compressionCodecs);
  }
  public long getPos() throws IOException { return this.seek.getPos(); }
  public void seek(long pos) throws IOException { this.seek.seek(pos); } 
  public boolean seekToNewSource(long targetPos) throws IOException { return this.seek.seekToNewSource(targetPos); }
  @Override public String toString() {
    return this.in.toString();
  }
}
