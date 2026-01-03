package zipserver

import (
	"fmt"
	"path"
	"strings"
)

// ResourceSpec contains all the info for an HTTP resource relevant for
// setting http headers and keeping track of the extraction work
type ResourceSpec struct {
	size            uint64
	key             string
	contentType     string
	contentEncoding string
}

func (rs *ResourceSpec) String() string {
	formattedEncoding := ""
	if rs.contentEncoding != "" {
		formattedEncoding = fmt.Sprintf(", %s encoding", rs.contentEncoding)
	}

	return fmt.Sprintf("%s (%s%s)", rs.key, rs.contentType, formattedEncoding)
}

// ToPutOptions returns PutOptions for storing this resource
func (rs *ResourceSpec) ToPutOptions() PutOptions {
	return PutOptions{
		ContentType:     rs.contentType,
		ContentEncoding: rs.contentEncoding,
		ACL:             ACLPublicRead,
	}
}

// RewriteSpec contains rules for rewriting file extensions
type RewriteSpec struct {
	oldExtension string
	newExtension string
}

var rewriteSpecs = []RewriteSpec{
	// // For Unity WebGL up to 5.5, see
	// // https://docs.unity3d.com/550/Documentation/Manual/webgl-deploying.html
	{".jsgz", ".js"},
	{".datagz", ".data"},
	{".memgz", ".mem"},
	{".unity3dgz", ".unity3d"},
}

func (rs *ResourceSpec) applyRewriteRules() {
	// rewrite rules only apply when we've identified the gzip suffix
	if rs.contentEncoding != "gzip" {
		return
	}

	extension := path.Ext(rs.key)

	for _, spec := range rewriteSpecs {
		if extension == spec.oldExtension {
			rs.key = strings.TrimSuffix(rs.key, spec.oldExtension) + spec.newExtension
			// only apply one rule at most
			return
		}
	}
}
