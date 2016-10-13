FROM flynn/cedarish

ADD builder/buildpacks.txt /build/
ADD builder/install-buildpack /build/
# Explicitly number the buildpacks directory based on the order of buildpacks.txt
RUN nl -nrz /build/buildpacks.txt | awk '{print $2 "\t" $1}' | xargs -L 1 /build/install-buildpack /build/buildpacks

RUN apt-get install --yes squashfs-tools

ADD convert-legacy-slug.sh /bin/convert-legacy-slug.sh
ADD bin/create-artifact /bin/create-artifact
ADD builder/build.sh /tmp/builder/

ENTRYPOINT ["/tmp/builder/build.sh"]
CMD []
