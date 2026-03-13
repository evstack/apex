package s3

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

type Server struct {
	svc    *Service
	log    zerolog.Logger
	region string
}

func NewServer(svc *Service, region string, log zerolog.Logger) *Server {
	return &Server{
		svc:    svc,
		log:    log.With().Str("component", "s3-server").Logger(),
		region: region,
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.log.Debug().Str("method", r.Method).Str("path", r.URL.Path).Msg("request")

	bucket, key := parsePath(r.URL.Path)
	query := r.URL.Query()

	switch {
	case bucket == "" && key == "":
		s.handleService(r, w)
	case bucket != "" && key == "":
		if query.Get("list-type") != "" || query.Has("prefix") || query.Has("delimiter") {
			s.handleListObjects(r, w, bucket)
		} else if r.Method == http.MethodGet {
			s.handleBucket(r, w, bucket)
		} else if r.Method == http.MethodPut {
			s.handleCreateBucket(r, w, bucket)
		} else if r.Method == http.MethodDelete {
			s.handleDeleteBucket(r, w, bucket)
		} else if r.Method == http.MethodHead {
			s.handleHeadBucket(r, w, bucket)
		} else {
			s.writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "")
		}
	case bucket != "" && key != "":
		s.handleObject(r, w, bucket, key)
	default:
		s.writeError(w, http.StatusBadRequest, "InvalidRequest", "invalid path")
	}
}

func parsePath(p string) (bucket, key string) {
	p = strings.TrimPrefix(p, "/")
	if p == "" {
		return "", ""
	}
	parts := strings.SplitN(p, "/", 2)
	bucket = parts[0]
	if len(parts) > 1 {
		key = parts[1]
	}
	return bucket, key
}

func (s *Server) handleService(r *http.Request, w http.ResponseWriter) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "")
		return
	}

	buckets, err := s.svc.ListBuckets(r.Context())
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	type BucketXML struct {
		Name         string `xml:"Name"`
		CreationDate string `xml:"CreationDate"`
	}
	type ListAllMyBucketsResult struct {
		XMLName xml.Name `xml:"ListAllMyBucketsResult"`
		Xmlns   string   `xml:"xmlns,attr"`
		Owner   struct {
			ID          string `xml:"ID"`
			DisplayName string `xml:"DisplayName"`
		} `xml:"Owner"`
		Buckets struct {
			Bucket []BucketXML `xml:"Bucket"`
		} `xml:"Buckets"`
	}

	result := ListAllMyBucketsResult{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/"}
	result.Owner.ID = "apex"
	result.Owner.DisplayName = "apex"
	for _, b := range buckets {
		result.Buckets.Bucket = append(result.Buckets.Bucket, BucketXML{
			Name:         b.Name,
			CreationDate: b.CreatedAt.UTC().Format(time.RFC3339),
		})
	}

	s.writeXML(w, result)
}

func (s *Server) handleBucket(r *http.Request, w http.ResponseWriter, bucket string) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "")
		return
	}
	s.handleListObjects(r, w, bucket)
}

func (s *Server) handleListObjects(r *http.Request, w http.ResponseWriter, bucket string) {
	query := r.URL.Query()
	prefix := query.Get("prefix")
	delimiter := query.Get("delimiter")
	marker := query.Get("marker")
	maxKeys := 1000
	if mk := query.Get("max-keys"); mk != "" {
		if n, err := strconv.Atoi(mk); err == nil && n > 0 {
			maxKeys = n
		}
	}

	result, err := s.svc.ListObjects(r.Context(), bucket, prefix, delimiter, marker, maxKeys)
	if err != nil {
		s.writeS3Error(w, err)
		return
	}

	type Contents struct {
		Key          string `xml:"Key"`
		LastModified string `xml:"LastModified"`
		ETag         string `xml:"ETag"`
		Size         int64  `xml:"Size"`
		StorageClass string `xml:"StorageClass"`
	}
	type CommonPrefix struct {
		Prefix string `xml:"Prefix"`
	}
	type ListBucketResult struct {
		XMLName        xml.Name `xml:"ListBucketResult"`
		Xmlns          string   `xml:"xmlns,attr"`
		Name           string   `xml:"Name"`
		Prefix         string   `xml:"Prefix"`
		Marker         string   `xml:"Marker"`
		MaxKeys        int      `xml:"MaxKeys"`
		IsTruncated    bool     `xml:"IsTruncated"`
		Contents       []Contents
		CommonPrefixes []CommonPrefix `xml:",omitempty"`
	}

	xmlResult := ListBucketResult{
		Xmlns:       "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:        result.Bucket,
		Prefix:      result.Prefix,
		Marker:      marker,
		MaxKeys:     maxKeys,
		IsTruncated: result.IsTruncated,
	}
	for _, obj := range result.Objects {
		xmlResult.Contents = append(xmlResult.Contents, Contents{
			Key:          obj.Key,
			LastModified: obj.LastModified.UTC().Format(time.RFC3339),
			ETag:         fmt.Sprintf(`"%s"`, obj.ETag),
			Size:         obj.Size,
			StorageClass: obj.StorageClass,
		})
	}
	for _, cp := range result.CommonPrefixes {
		xmlResult.CommonPrefixes = append(xmlResult.CommonPrefixes, CommonPrefix{Prefix: cp})
	}

	s.writeXML(w, xmlResult)
}

func (s *Server) handleCreateBucket(r *http.Request, w http.ResponseWriter, bucket string) {
	if r.Method != http.MethodPut {
		s.writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "")
		return
	}

	err := s.svc.CreateBucket(r.Context(), bucket)
	if err != nil {
		s.writeS3Error(w, err)
		return
	}

	w.Header().Set("Location", "/"+bucket)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDeleteBucket(r *http.Request, w http.ResponseWriter, bucket string) {
	if r.Method != http.MethodDelete {
		s.writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "")
		return
	}

	err := s.svc.DeleteBucket(r.Context(), bucket)
	if err != nil {
		s.writeS3Error(w, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleHeadBucket(r *http.Request, w http.ResponseWriter, bucket string) {
	if r.Method != http.MethodHead {
		s.writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "")
		return
	}

	_, err := s.svc.HeadBucket(r.Context(), bucket)
	if err != nil {
		s.writeS3Error(w, err)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleObject(r *http.Request, w http.ResponseWriter, bucket, key string) {
	switch r.Method {
	case http.MethodGet:
		s.handleGetObject(r, w, bucket, key)
	case http.MethodPut:
		s.handlePutObject(r, w, bucket, key)
	case http.MethodDelete:
		s.handleDeleteObject(r, w, bucket, key)
	case http.MethodHead:
		s.handleHeadObject(r, w, bucket, key)
	default:
		s.writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "")
	}
}

func (s *Server) handleGetObject(r *http.Request, w http.ResponseWriter, bucket, key string) {
	obj, data, err := s.svc.GetObject(r.Context(), bucket, key)
	if err != nil {
		s.writeS3Error(w, err)
		return
	}

	w.Header().Set("Content-Type", obj.ContentType)
	if obj.ContentType == "" {
		w.Header().Set("Content-Type", "application/octet-stream")
	}
	w.Header().Set("Content-Length", strconv.FormatInt(obj.Size, 10))
	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, obj.ETag))
	w.Header().Set("Last-Modified", obj.LastModified.UTC().Format(http.TimeFormat))
	w.Write(data)
}

func (s *Server) handlePutObject(r *http.Request, w http.ResponseWriter, bucket, key string) {
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	obj, err := s.svc.PutObject(r.Context(), bucket, key, r.Body, contentType)
	if err != nil {
		s.writeS3Error(w, err)
		return
	}

	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, obj.ETag))
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDeleteObject(r *http.Request, w http.ResponseWriter, bucket, key string) {
	err := s.svc.DeleteObject(r.Context(), bucket, key)
	if err != nil {
		s.writeS3Error(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleHeadObject(r *http.Request, w http.ResponseWriter, bucket, key string) {
	obj, err := s.svc.HeadObject(r.Context(), bucket, key)
	if err != nil {
		s.writeS3Error(w, err)
		return
	}

	w.Header().Set("Content-Type", obj.ContentType)
	if obj.ContentType == "" {
		w.Header().Set("Content-Type", "application/octet-stream")
	}
	w.Header().Set("Content-Length", strconv.FormatInt(obj.Size, 10))
	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, obj.ETag))
	w.Header().Set("Last-Modified", obj.LastModified.UTC().Format(http.TimeFormat))
	w.WriteHeader(http.StatusOK)
}

func (s *Server) writeXML(w http.ResponseWriter, data any) {
	w.Header().Set("Content-Type", "application/xml")
	output, err := xml.Marshal(data)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	w.Write([]byte(xml.Header))
	w.Write(output)
}

func (s *Server) writeS3Error(w http.ResponseWriter, err error) {
	switch {
	case err == ErrBucketNotFound:
		s.writeError(w, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist")
	case err == ErrBucketNotEmpty:
		s.writeError(w, http.StatusConflict, "BucketNotEmpty", "The bucket you tried to delete is not empty")
	case err == ErrBucketAlreadyExists:
		s.writeError(w, http.StatusConflict, "BucketAlreadyExists", "The requested bucket name is not available")
	case err == ErrObjectNotFound:
		s.writeError(w, http.StatusNotFound, "NoSuchKey", "The specified key does not exist")
	case err == ErrObjectTooLarge:
		s.writeError(w, http.StatusRequestEntityTooLarge, "EntityTooLarge", "Your proposed upload exceeds the maximum allowed size")
	default:
		s.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
	}
}

func (s *Server) writeError(w http.ResponseWriter, code int, codeStr, message string) {
	type Error struct {
		XMLName   xml.Name `xml:"Error"`
		Code      string   `xml:"Code"`
		Message   string   `xml:"Message"`
		Resource  string   `xml:"Resource"`
		RequestID string   `xml:"RequestId"`
	}
	err := Error{
		Code:      codeStr,
		Message:   message,
		RequestID: "apex",
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(code)
	output, _ := xml.Marshal(err)
	w.Write([]byte(xml.Header))
	w.Write(output)
}
