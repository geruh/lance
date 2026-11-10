/*
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
package org.lance.fragment;

import com.google.common.base.MoreObjects;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

public class DeletionFile implements Serializable {
  private static final long serialVersionUID = 3786348766842875859L;

  private final long id;
  private final long readVersion;
  private final Long numDeletedRows;
  private final DeletionFileType fileType;
  private final Integer baseId;
  private final String path;

  public DeletionFile(
      long id, long readVersion, Long numDeletedRows, DeletionFileType fileType, Integer baseId) {
    this(id, readVersion, numDeletedRows, fileType, baseId, null);
  }

  /**
   * @param path the explicit path of the file relative to its dataset-root base, {@code
   *     _deletions/{uuid}.arrow} or {@code _deletions/{uuid}.bin} to match {@code fileType}, with
   *     the UUID in lowercase hyphenated form; or null to locate the file by the id of the fragment
   *     that owns it. Lance rejects any other value when the file metadata is passed to it.
   *     Experimental: committing a table that uses it requires {@code
   *     LANCE_ENABLE_UNSTABLE_DELETION_FILE_PATHS} in release builds.
   */
  public DeletionFile(
      long id,
      long readVersion,
      Long numDeletedRows,
      DeletionFileType fileType,
      Integer baseId,
      String path) {
    this.id = id;
    this.readVersion = readVersion;
    this.numDeletedRows = numDeletedRows;
    this.fileType = fileType;
    this.baseId = baseId;
    this.path = path;
  }

  public long getId() {
    return id;
  }

  public long getReadVersion() {
    return readVersion;
  }

  public Long getNumDeletedRows() {
    return numDeletedRows;
  }

  public DeletionFileType getFileType() {
    return fileType;
  }

  public Optional<Integer> getBaseId() {
    return Optional.ofNullable(baseId);
  }

  /**
   * Returns the explicit path of the file relative to its dataset-root base, or empty when the
   * location is derived from the id of the fragment that owns the file.
   */
  public Optional<String> getPath() {
    return Optional.ofNullable(path);
  }

  /**
   * Returns the deletion file path relative to its dataset-root base.
   *
   * @param fragmentId fragment that owns this deletion file, which must be non-negative; ignored
   *     when the file has an explicit path
   */
  public String getRelativePath(long fragmentId) {
    if (path != null) {
      return path;
    }
    if (fragmentId < 0) {
      throw new IllegalArgumentException("fragmentId must be non-negative");
    }
    String suffix;
    switch (fileType) {
      case ARRAY:
        suffix = "arrow";
        break;
      case BITMAP:
        suffix = "bin";
        break;
      default:
        throw new IllegalStateException("Unsupported deletion file type: " + fileType);
    }
    return "_deletions/"
        + fragmentId
        + "-"
        + readVersion
        + "-"
        + Long.toUnsignedString(id)
        + "."
        + suffix;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DeletionFile that = (DeletionFile) o;
    return id == that.id
        && readVersion == that.readVersion
        && fileType == that.fileType
        && Objects.equals(numDeletedRows, that.numDeletedRows)
        && Objects.equals(path, that.path);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, readVersion, fileType, numDeletedRows, path);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("id", id)
        .add("readVersion", readVersion)
        .add("numDeletedRows", numDeletedRows)
        .add("fileType", fileType)
        .add("baseId", baseId)
        .add("path", path)
        .toString();
  }
}
