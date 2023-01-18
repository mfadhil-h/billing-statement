package main

import (
	"fmt"
	"github.com/jung-kurt/gofpdf"
	"log"
)

func createHeader(pdf *gofpdf.Fpdf) {
	pdf.SetFont("Arial", "B", 16)
	pdf.Text(10, 10, "INVOICE")

	pdf.SetFont("Arial", "B", 8)
	pdf.Text(10, 20, "From :")

	pdf.SetFont("Arial", "", 6)
	pdf.Text(10, 25, "PT. Pintar Cari Usaha")
	pdf.Text(10, 28, "Address 1")
	pdf.Text(10, 31, "Address 2")
	pdf.Text(10, 34, "City")
	pdf.Text(10, 37, "Email")
	pdf.Text(10, 40, "Phone")


	pdf.SetFont("Arial", "B", 8)
	pdf.Text(80, 20, "To :")

	pdf.SetFont("Arial", "", 6)
	pdf.Text(80, 25, "PT. Ceria Cari Cuan")
	pdf.Text(80, 28, "Address 1")
	pdf.Text(80, 31, "Address 2")
	pdf.Text(80, 34, "City")
	pdf.Text(80, 37, "Email")
	pdf.Text(80, 40, "Phone")

	pdf.Image("D:\\pintar.jpg", 160, 10, 35, 12, false, "", 0, "")


}

func createTable(pdf *gofpdf.Fpdf, header []string, data [][]string, columnSize []float64) {
	if pdf == nil {
		return
	}
	fmt.Println(header)
	fmt.Println("---")
	fmt.Println(data)
	fmt.Println("---")
	fmt.Println(columnSize)
	fmt.Println("---")

	cLen := len(columnSize)
	if cLen == 0 {
		return
	}
	fmt.Println("cLen > 0")
	if len(header) != 0 && cLen != len(header) {
		return
	}
	fmt.Println("len header 0 or eq to cLen")
	for _, v := range data {
		if len(v) != cLen {
			return
		}
	}
	fmt.Println("Check done")
	pdf.SetFillColor(128, 128, 128)
	pdf.SetTextColor(255, 255, 255)
	pdf.SetDrawColor(128, 0, 0)
	pdf.SetLineWidth(.3)
	pdf.SetFont("Arial", "", 12)
	// display header fields
	for i, str := range header {
		pdf.CellFormat(columnSize[i], 7, str, "1", 0, "C", true, 0, "")
	}
	// only advance a line if we wrote a header
	if len(header) > 0 {
		pdf.Ln(-1)
	}
	// display data /w alternating background
	pdf.SetFillColor(224, 235, 255)
	pdf.SetTextColor(0, 0, 0)
	pdf.SetFont("", "", 0)
	//  Data
	fill := false
	for _, c := range data {
		for i, str := range c {
			pdf.CellFormat(columnSize[i], 6, str, "LR", 0, "", fill, 0, "")
		}
		pdf.Ln(-1)
		fill = !fill
	}


}

func main() {
	pdf := gofpdf.New("P", "mm", "A4", "")
	pdf.AddPage()

	//pdf.SetFont("Arial", "B", 16)
	//pdf.Text(40, 10, "Hello, world")
	//pdf.Image("D:\\rhnimages.jpg", 56, 40, 100, 100, false, "", 0, "")
	//
	//arrHeader := []string{"halo", "test"}
	//arrData := [][]string{{"abc", "def"},{"ghi", "jkl"},{"asd", "fgh"},{"jjj", "ooo"}}
	//arrColomn := []float64{30.0, 40.5}

	//createTable(pdf, arrHeader, arrData, arrColomn)

	createHeader(pdf)

	err := pdf.OutputFileAndClose("D:\\aafile.pdf")
	if err != nil {
		log.Println("ERROR", err.Error())
	}

}