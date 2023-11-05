'use client'

import Link from 'next/link'
import { Inter } from 'next/font/google'
import { Navbar, Button, Alignment } from '@blueprintjs/core'
import './globals.css'
import '@blueprintjs/core/lib/css/blueprint.css'

const inter = Inter({ subsets: ['latin'] })

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <body className={inter.className}>
        <Navbar fixedToTop className="bp5-dark">
          <Navbar.Group align={Alignment.LEFT}>
            <Navbar.Heading>Blueprint</Navbar.Heading>
            <Navbar.Divider />
            <Link href="/status"><Button className="bp5-minimal" icon="pulse" text="Status"  /></Link>
            <Link href="/runs"><Button className="bp5-minimal" icon="walk" text="Runs" /></Link>
            <Button className="bp5-minimal" icon="multi-select" text="Partitions" />          
          </Navbar.Group>
        </Navbar>
        {children}
      </body>
    </html>
  )
}
