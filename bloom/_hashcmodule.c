#define PY_SSIZE_T_CLEAN
#include <Python.h>

static PyObject* hashc_hello(PyObject *self, PyObject *args) {
    const char *msg;
    if (!PyArg_ParseTuple(args, "s", &msg)) {
        return NULL;
    }
    return PyLong_FromLong(strlen(msg));
}

static uint32_t fnv1a_32(const unsigned char *data, size_t size) {
    uint32_t hval = 2166136261UL;
    const uint32_t prime = 16777619UL;
    for (size_t i = 0; i < size; ++i) {
        hval = (hval ^ data[i]) * prime;
    }
    return hval;
}

static PyObject* hashc_fnv1a_32(PyObject *self, PyObject *args) {
    const unsigned char *sample;
    Py_ssize_t sampleSize;
    if (!PyArg_ParseTuple(args, "s#", &sample, &sampleSize)) {
	    return NULL;
    }
    uint32_t h = fnv1a_32(sample, sampleSize);
    return PyLong_FromLong(h);
}

static uint64_t fnv1a_64(const unsigned char *data, size_t size) {
    uint64_t hval = 14695981039346656037ULL;
    const uint64_t prime = 1099511628211ULL;
    for (size_t i = 0; i < size; ++i) {
        hval = (hval ^ data[i]) * prime;
    }
    return hval;
}

static PyObject* hashc_fnv1a_64(PyObject *self, PyObject *args) {
    const char *sample;
    Py_ssize_t sampleSize;
    if (!PyArg_ParseTuple(args, "s#", &sample, &sampleSize)) {
	    return NULL;
    }
    uint64_t h = fnv1a_64(sample, sampleSize);
    return PyLong_FromUnsignedLongLong(h);

}

static PyMethodDef methods[] = {
    {"hello", hashc_hello, METH_VARARGS, "Sample function."},
    {"fnv1a_32", hashc_fnv1a_32, METH_VARARGS, "FNV-1A 32-bit version."},
    {"fnv1a_64", hashc_fnv1a_64, METH_VARARGS, "FNV-1A 64-bit version."},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef hashcmodule = {
    PyModuleDef_HEAD_INIT,
    "hashc",
    NULL,
    -1,
    methods
};

PyMODINIT_FUNC PyInit__hashc(void) {
    return PyModule_Create(&hashcmodule);
}
