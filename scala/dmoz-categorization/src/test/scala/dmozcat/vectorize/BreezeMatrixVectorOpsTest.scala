package dmozcat.vectorize

import org.junit.Test
import breeze.linalg.DenseMatrix
import breeze.linalg.DenseVector

class BreezeMatrixVectorOpsTest {

//    @Test
//    def buildMatrix(): Unit = {
//        val m = new DenseMatrix(3, 4, Array[Double](1, 2, 3, 
//            4, 5, 6, 7, 8, 9, 10, 11, 12))
//        val v = new DenseVector(Array[Double](1, 2, 3, 4))
//        Console.println(m)
//        Console.println(v)
//        val mv = m * v
//        Console.println(mv)
//    }
    
    @Test
    def testHCat(): Unit = {
        val v1 = new DenseVector(Array[Double](1, 2, 3, 4))
        val v2 = new DenseVector(Array[Double](5, 6, 7, 8))
        val v3 = new DenseVector(Array[Double](9, 10, 11, 12))
        val vs = List(v1, v2, v3)
        Console.println(vs)
        val vcat = vs.reduce((a, b) => DenseVector.vertcat(a, b))
        Console.println(vcat)
        val m = new DenseMatrix(v1.size, 3, vcat.toArray)
        Console.println(m.t)
        val q = new DenseVector(Array[Double](1, 2, 3, 4))
        val mult = m.t * q
        Console.println(mult)
    }
}
