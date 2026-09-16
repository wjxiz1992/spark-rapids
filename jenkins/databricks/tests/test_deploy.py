# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import unittest
import xml.etree.ElementTree as ET
import zipfile


REPO = Path(__file__).resolve().parents[3]
MANIFEST = REPO / "dist/root-safe-module-classes.txt"
VERSION = ET.parse(REPO / "pom.xml").getroot().find(
    "{http://maven.apache.org/POM/4.0.0}version").text

# Run the actual deployment script with a Maven test double.
# Validate the artifact and POM paths as well as recording requested deployments.
FAKE_MAVEN = """#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys
import xml.etree.ElementTree as ET

args = sys.argv[1:]
if 'help:evaluate' in args:
    print(os.environ['TEST_VERSION'])
    sys.exit(0)
assert 'deploy:deploy-file' in args, args
props = dict(arg[2:].split('=', 1) for arg in args if arg.startswith('-D') and '=' in arg)
project = Path(args[args.index('-f') + 1]) if '-f' in args else Path('.')
jar = project / props['file']
assert jar.is_file(), 'Missing artifact: ' + str(jar)
pom = ET.parse(project / props['pomFile']).getroot()
artifact = pom.find('{http://maven.apache.org/POM/4.0.0}artifactId').text
assert jar.name == artifact + '-' + os.environ['TEST_VERSION'] + '-' + props['classifier'] + '.jar'
with open(os.environ['TEST_DEPLOYMENTS'], 'a') as output:
    output.write(json.dumps({'artifact': artifact, 'classifier': props['classifier']}) + '\\n')
"""


class DatabricksDeployTest(unittest.TestCase):
    def run_deploy(self, spark, runtime, classifier, missing=None, extra_module=None):
        scala = "2.13" if spark.startswith("4.") else "2.12"
        helpers = [line.strip() for line in MANIFEST.read_text().splitlines()
                   if line.strip() and not line.lstrip().startswith("#")]
        if extra_module:
            helpers.append(extra_module)
        modules = ["aggregator", "sql-plugin-api", "integration_tests"] + helpers
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "spark-rapids"
            project = source / "scala2.13" if scala == "2.13" else source
            (source / "dist").mkdir(parents=True)
            manifest = MANIFEST.read_text()
            if extra_module:
                manifest += "\n  # another root-safe module\n\n  " + extra_module + "  "
            (source / "dist/root-safe-module-classes.txt").write_text(manifest)
            expected = set()
            for module in modules:
                module_dir = project / module
                module_dir.mkdir(parents=True)
                if module == extra_module:
                    artifact = "rapids-4-spark-" + module + "_" + scala
                    (module_dir / "pom.xml").write_text(
                        '<project xmlns="http://maven.apache.org/POM/4.0.0">'
                        '<artifactId>' + artifact + '</artifactId></project>')
                else:
                    original = REPO / "scala2.13" if scala == "2.13" else REPO
                    shutil.copyfile(original / module / "pom.xml", module_dir / "pom.xml")
                    artifact = ET.parse(module_dir / "pom.xml").getroot().find(
                        "{http://maven.apache.org/POM/4.0.0}artifactId").text
                expected.add(artifact)
                target = module_dir / "target"
                if module != "integration_tests":
                    target /= classifier
                target.mkdir(parents=True)
                if module != missing:
                    with zipfile.ZipFile(target / (artifact + "-" + VERSION + "-" +
                                                  classifier + ".jar"), "w") as jar:
                        jar.writestr("META-INF/MANIFEST.MF", "Manifest-Version: 1.0\n")
            with tarfile.open(root / "spark-rapids-built.tgz", "w:gz") as bundle:
                bundle.add(source, arcname="spark-rapids")
            # All artifacts must come from the extracted build bundle.
            shutil.rmtree(source)
            bin_dir = root / "bin"
            bin_dir.mkdir()
            mvn = bin_dir / "mvn"
            mvn.write_text(FAKE_MAVEN)
            mvn.chmod(0o755)
            deployments = root / "deployments.jsonl"
            deployments.touch()
            env = {
                "PATH": str(bin_dir) + os.pathsep + os.environ["PATH"],
                "BASE_SPARK_VERSION": spark,
                "BASE_SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS": spark,
                "DB_RUNTIME": runtime,
                "ART_URL": "https://example.invalid/snapshots",
                "TEST_VERSION": VERSION,
                "TEST_DEPLOYMENTS": str(deployments),
            }
            result = subprocess.run(["bash", str(REPO / "jenkins/databricks/deploy.sh")],
                                    cwd=root, env=env, capture_output=True, text=True)
            records = [json.loads(line) for line in deployments.read_text().splitlines()]
            return result, records, expected

    def assert_complete_deployment(self, spark, runtime, classifier, **kwargs):
        result, records, expected = self.run_deploy(spark, runtime, classifier, **kwargs)
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual(expected, {record["artifact"] for record in records})
        self.assertEqual(len(expected), len(records))
        self.assertEqual({classifier}, {record["classifier"] for record in records})

    def test_dbr143_deploys_every_packager_dependency(self):
        self.assert_complete_deployment("3.5.0", "14.3.x-gpu-ml-scala2.12", "spark350db143")

    def test_dbr154_keeps_unsuffixed_classifier(self):
        self.assert_complete_deployment("3.5.0", "15.4.x-gpu-ml-scala2.12", "spark350db")

    def test_dbr173_uses_scala213_artifacts_and_poms(self):
        self.assert_complete_deployment("4.0.0", "17.3.x-gpu-ml-scala2.13", "spark400db173")

    def test_new_root_safe_module_is_deployed(self):
        self.assert_complete_deployment("3.5.0", "14.3.x", "spark350db143",
                                        extra_module="future-helper")

    def test_missing_helper_fails_deployment(self):
        result, _, _ = self.run_deploy("3.5.0", "14.3.x", "spark350db143",
                                       missing="sql-plugin-columnar")
        self.assertNotEqual(0, result.returncode)
        self.assertIn("Missing artifact:", result.stderr)
        self.assertIn("sql-plugin-columnar", result.stderr)


if __name__ == "__main__":
    unittest.main()
